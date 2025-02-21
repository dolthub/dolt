// Copyright 2025 Dolthub, Inc.
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
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jpillora/backoff"
)

// s3ObjectReader is a wrapper for S3 that gives us some nice to haves for reading objects from S3.
// TODO: Bring all the multipart upload and remote-conjoin stuff over here and make this a better analogue to ddbTableStore
type s3ObjectReader struct {
	s3     s3iface.S3API
	bucket string
	readRl chan struct{}
	ns     string
}

// httpEndRangeHeader returns a string for the HTTP range header value (eg "bytes=-42") to retrieve the last |length| bytes
// of the object. Intended to be used with the |readRange| method, to set Range of request.
func httpEndRangeHeader(length int) string {
	return fmt.Sprintf("%s=-%d", s3RangePrefix, length)
}

// httpRangeHeader returns a string for the HTTP range header value (eg "bytes=23-42") for a range starting at |off|
// with a length of |length|. Intended to be used with the |readRange| method, to set Range of request.
func httpRangeHeader(off, length int64) string {
	lastByte := off + length - 1 // insanely, the HTTP range header specifies ranges inclusively.
	return fmt.Sprintf("%s=%d-%d", s3RangePrefix, off, lastByte)
}

func (s3or *s3ObjectReader) key(name string) string {
	if s3or.ns != "" {
		return s3or.ns + "/" + name
	}
	return name
}

// ReadAt gets the named object, and reads |len(p)| bytes into |p| starting at |off|. The number of bytes read is returned,
// along with an error if one occurs.
func (s3or *s3ObjectReader) ReadAt(ctx context.Context, name string, p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()

	defer func() {
		stats.S3BytesPerRead.Sample(uint64(len(p)))
		stats.S3ReadLatency.SampleTimeSince(t1)
	}()

	n, _, err = s3or.readRange(ctx, name, p, httpRangeHeader(off, int64(len(p))))
	return
}

// reader gets the full object from S3 as a ReadCloser. Useful when downloading the entire object.
func (s3or *s3ObjectReader) reader(ctx context.Context, name string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s3or.bucket),
		Key:    aws.String(s3or.key(name)),
	}
	result, err := s3or.s3.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

// readRange implements the raw calls to S3 for the purpose of reading a range of bytes from an |name| object. Contents
// are read into |p| and the range is specified as a string, which you should get using the |httpRangeHeader| function.
// The return value is the number of bytes |n| read and the total size |sz| of the object. The size of the object comes from the Content-Range
// HTTP header, and can be used if manually breaking of the file into range chunks
func (s3or *s3ObjectReader) readRange(ctx context.Context, name string, p []byte, rangeHeader string) (n int, sz uint64, err error) {
	read := func() (int, uint64, error) {
		if s3or.readRl != nil {
			s3or.readRl <- struct{}{}
			defer func() {
				<-s3or.readRl
			}()
		}

		input := &s3.GetObjectInput{
			Bucket: aws.String(s3or.bucket),
			Key:    aws.String(s3or.key(name)),
			Range:  aws.String(rangeHeader),
		}

		result, err := s3or.s3.GetObjectWithContext(ctx, input)
		if err != nil {
			return 0, 0, err
		}
		defer result.Body.Close()

		if *result.ContentLength != int64(len(p)) {
			return 0, 0, fmt.Errorf("failed to read entire range, key: %v, len(p): %d, rangeHeader: %s, ContentLength: %d", s3or.key(name), len(p), rangeHeader, *result.ContentLength)
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
