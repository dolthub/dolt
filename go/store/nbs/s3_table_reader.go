// Copyright 2019-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jpillora/backoff"
	"golang.org/x/sync/errgroup"
)

const (
	s3RangePrefix = "bytes"
	s3BlockSize   = (1 << 10) * 512 // 512K
)

type s3TableReaderAt struct {
	s3 *s3ObjectReader
	h  addr
}

type s3svc interface {
	AbortMultipartUploadWithContext(ctx aws.Context, input *s3.AbortMultipartUploadInput, opts ...request.Option) (*s3.AbortMultipartUploadOutput, error)
	CreateMultipartUploadWithContext(ctx aws.Context, input *s3.CreateMultipartUploadInput, opts ...request.Option) (*s3.CreateMultipartUploadOutput, error)
	UploadPartWithContext(ctx aws.Context, input *s3.UploadPartInput, opts ...request.Option) (*s3.UploadPartOutput, error)
	UploadPartCopyWithContext(ctx aws.Context, input *s3.UploadPartCopyInput, opts ...request.Option) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUploadWithContext(ctx aws.Context, input *s3.CompleteMultipartUploadInput, opts ...request.Option) (*s3.CompleteMultipartUploadOutput, error)
	GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error)
	PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error)
}

func (s3tra *s3TableReaderAt) ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error) {
	return s3tra.s3.ReadAt(ctx, s3tra.h, p, off, stats)
}

// TODO: Bring all the multipart upload and remote-conjoin stuff over here and make this a better analogue to ddbTableStore
type s3ObjectReader struct {
	s3     s3svc
	bucket string
	readRl chan struct{}
	ns     string
}

func (s3or *s3ObjectReader) key(k string) string {
	if s3or.ns != "" {
		return s3or.ns + "/" + k
	}
	return k
}

func (s3or *s3ObjectReader) ReadAt(ctx context.Context, name addr, p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()

	defer func() {
		stats.S3BytesPerRead.Sample(uint64(len(p)))
		stats.S3ReadLatency.SampleTimeSince(t1)
	}()

	n, _, err = s3or.readRange(ctx, name, p, s3RangeHeader(off, int64(len(p))))
	return
}

func s3RangeHeader(off, length int64) string {
	lastByte := off + length - 1 // insanely, the HTTP range header specifies ranges inclusively.
	return fmt.Sprintf("%s=%d-%d", s3RangePrefix, off, lastByte)
}

const maxS3ReadFromEndReqSize = 256 * 1024 * 1024       // 256MB
const preferredS3ReadFromEndReqSize = 128 * 1024 * 1024 // 128MB

func (s3or *s3ObjectReader) ReadFromEnd(ctx context.Context, name addr, p []byte, stats *Stats) (n int, sz uint64, err error) {
	defer func(t1 time.Time) {
		stats.S3BytesPerRead.Sample(uint64(len(p)))
		stats.S3ReadLatency.SampleTimeSince(t1)
	}(time.Now())
	totalN := uint64(0)
	if len(p) > maxS3ReadFromEndReqSize {
		// If we're bigger than 256MB, parallelize the read...
		// Read the footer first and capture the size of the entire table file.
		n, sz, err := s3or.readRange(ctx, name, p[len(p)-footerSize:], fmt.Sprintf("%s=-%d", s3RangePrefix, footerSize))
		if err != nil {
			return n, sz, err
		}
		totalN += uint64(n)
		eg, egctx := errgroup.WithContext(ctx)
		start := 0
		for start < len(p)-footerSize {
			// Make parallel read requests of up to 128MB.
			end := start + preferredS3ReadFromEndReqSize
			if end > len(p)-footerSize {
				end = len(p) - footerSize
			}
			bs := p[start:end]
			rangeStart := sz - uint64(len(p)) + uint64(start)
			rangeEnd := sz - uint64(len(p)) + uint64(end) - 1
			eg.Go(func() error {
				n, _, err := s3or.readRange(egctx, name, bs, fmt.Sprintf("%s=%d-%d", s3RangePrefix, rangeStart, rangeEnd))
				if err != nil {
					return err
				}
				atomic.AddUint64(&totalN, uint64(n))
				return nil
			})
			start = end
		}
		err = eg.Wait()
		if err != nil {
			return 0, 0, err
		}
		return int(totalN), sz, nil
	}
	return s3or.readRange(ctx, name, p, fmt.Sprintf("%s=-%d", s3RangePrefix, len(p)))
}

func (s3or *s3ObjectReader) readRange(ctx context.Context, name addr, p []byte, rangeHeader string) (n int, sz uint64, err error) {
	read := func() (int, uint64, error) {
		if s3or.readRl != nil {
			s3or.readRl <- struct{}{}
			defer func() {
				<-s3or.readRl
			}()
		}

		input := &s3.GetObjectInput{
			Bucket: aws.String(s3or.bucket),
			Key:    aws.String(s3or.key(name.String())),
			Range:  aws.String(rangeHeader),
		}

		result, err := s3or.s3.GetObjectWithContext(ctx, input)
		if err != nil {
			return 0, 0, err
		}
		defer result.Body.Close()

		if *result.ContentLength != int64(len(p)) {
			return 0, 0, fmt.Errorf("failed to read entire range, key: %v, len(p): %d, rangeHeader: %s, ContentLength: %d", s3or.key(name.String()), len(p), rangeHeader, *result.ContentLength)
		}

		sz := uint64(0)
		if result.ContentRange != nil {
			i := strings.Index(*result.ContentRange, "/")
			if i != -1 {
				sz, err = strconv.ParseUint((*result.ContentRange)[i+1:], 10, 64)
				if err != nil {
					return 0, 0, err
				}
			}
		}

		n, err = io.ReadFull(result.Body, p)
		return n, sz, err
	}

	n, sz, err = read()
	// We hit the point of diminishing returns investigating #3255, so add retries. In conversations with AWS people, it's not surprising to get transient failures when talking to S3, though SDKs are intended to have their own retrying. The issue may be that, in Go, making the S3 request and reading the data are separate operations, and the SDK kind of can't do its own retrying to handle failures in the latter.
	if isConnReset(err) {
		// We are backing off here because its possible and likely that the rate of requests to S3 is the underlying issue.
		b := &backoff.Backoff{
			Min:    128 * time.Microsecond,
			Max:    1024 * time.Millisecond,
			Factor: 2,
			Jitter: true,
		}
		for ; isConnReset(err); n, sz, err = read() {
			dur := b.Duration()
			time.Sleep(dur)
		}
	}

	return n, sz, err
}

func isConnReset(err error) bool {
	nErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}
	scErr, ok := nErr.Err.(*os.SyscallError)
	return ok && scErr.Err == syscall.ECONNRESET
}
