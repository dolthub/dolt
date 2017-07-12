// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/attic-labs/noms/go/d"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jpillora/backoff"
)

const (
	s3RangePrefix = "bytes"
	s3BlockSize   = (1 << 10) * 512 // 512K
)

type s3TableReader struct {
	tableReader
	s3     s3svc
	bucket string
	h      addr
	readRl chan struct{}
	tc     tableCache
}

type s3svc interface {
	AbortMultipartUpload(input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)
	CreateMultipartUpload(input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(input *s3.UploadPartInput) (*s3.UploadPartOutput, error)
	UploadPartCopy(input *s3.UploadPartCopyInput) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

func newS3TableReader(s3 s3svc, bucket string, h addr, chunkCount uint32, indexCache *indexCache, readRl chan struct{}, tc tableCache) chunkSource {
	source := &s3TableReader{s3: s3, bucket: bucket, h: h, readRl: readRl, tc: tc}

	var index tableIndex
	found := false
	if indexCache != nil {
		indexCache.lockEntry(h)
		defer indexCache.unlockEntry(h)
		index, found = indexCache.get(h)
	}

	if !found {
		size := indexSize(chunkCount) + footerSize
		buff := make([]byte, size)

		n, err := source.readRange(buff, fmt.Sprintf("%s=-%d", s3RangePrefix, size))
		d.PanicIfError(err)
		d.PanicIfFalse(size == uint64(n))
		index = parseTableIndex(buff)

		if indexCache != nil {
			indexCache.put(h, index)
		}
	}

	source.tableReader = newTableReader(index, source, s3BlockSize)
	d.PanicIfFalse(chunkCount == source.count())
	return source
}

func (s3tr *s3TableReader) hash() addr {
	return s3tr.h
}

func (s3tr *s3TableReader) ReadAtWithStats(p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()

	if s3tr.tc != nil {
		r := s3tr.tc.checkout(s3tr.hash())
		if r != nil {
			defer func() {
				stats.FileBytesPerRead.Sample(uint64(len(p)))
				stats.FileReadLatency.SampleTimeSince(t1)
			}()
			defer s3tr.tc.checkin(s3tr.hash())
			return r.ReadAt(p, off)
		}
	}

	defer func() {
		stats.S3BytesPerRead.Sample(uint64(len(p)))
		stats.S3ReadLatency.SampleTimeSince(t1)
	}()
	return s3tr.readRange(p, s3RangeHeader(off, int64(len(p))))
}

func s3RangeHeader(off, length int64) string {
	lastByte := off + length - 1 // insanely, the HTTP range header specifies ranges inclusively.
	return fmt.Sprintf("%s=%d-%d", s3RangePrefix, off, lastByte)
}

func (s3tr *s3TableReader) readRange(p []byte, rangeHeader string) (n int, err error) {
	read := func() (int, error) {
		if s3tr.readRl != nil {
			s3tr.readRl <- struct{}{}
			defer func() {
				<-s3tr.readRl
			}()
		}

		input := &s3.GetObjectInput{
			Bucket: aws.String(s3tr.bucket),
			Key:    aws.String(s3tr.hash().String()),
			Range:  aws.String(rangeHeader),
		}
		result, err := s3tr.s3.GetObject(input)
		d.PanicIfError(err)
		d.PanicIfFalse(*result.ContentLength == int64(len(p)))

		n, err := io.ReadFull(result.Body, p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed ranged read from S3\n%s\nerr type: %T\nerror: %v\n", input.GoString(), err, err)
		}
		return n, err
	}

	n, err = read()
	// We hit the point of diminishing returns investigating #3255, so add retries. In conversations with AWS people, it's not surprising to get transient failures when talking to S3, though SDKs are intended to have their own retrying. The issue may be that, in Go, making the S3 request and reading the data are separate operations, and the SDK kind of can't do its own retrying to handle failures in the latter.
	if isConnReset(err) {
		// We are backing off here because its possible and likely that the rate of requests to S3 is the underlying issue.
		b := &backoff.Backoff{
			Min:    128 * time.Microsecond,
			Max:    1024 * time.Millisecond,
			Factor: 2,
			Jitter: true,
		}
		for ; isConnReset(err); n, err = read() {
			dur := b.Duration()
			fmt.Fprintf(os.Stderr, "Retrying S3 read in %s\n", dur.String())
			time.Sleep(dur)
		}
	}
	return
}

func isConnReset(err error) bool {
	nErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}
	scErr, ok := nErr.Err.(*os.SyscallError)
	return ok && scErr.Err == unix.ECONNRESET
}
