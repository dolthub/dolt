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

type s3TableReaderAt struct {
	s3 *s3ObjectReader
	h  addr
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

func (s3tra *s3TableReaderAt) ReadAtWithStats(p []byte, off int64, stats *Stats) (n int, err error) {
	return s3tra.s3.ReadAt(s3tra.h, p, off, stats)
}

// TODO: Bring all the multipart upload and remote-conjoin stuff over here and make this a better analogue to ddbTableStore
type s3ObjectReader struct {
	s3     s3svc
	bucket string
	readRl chan struct{}
	tc     tableCache
}

func (s3or *s3ObjectReader) ReadAt(name addr, p []byte, off int64, stats *Stats) (n int, err error) {
	t1 := time.Now()

	if s3or.tc != nil {
		r := s3or.tc.checkout(name)
		if r != nil {
			defer func() {
				stats.FileBytesPerRead.Sample(uint64(len(p)))
				stats.FileReadLatency.SampleTimeSince(t1)
			}()
			defer s3or.tc.checkin(name)
			return r.ReadAt(p, off)
		}
	}

	defer func() {
		stats.S3BytesPerRead.Sample(uint64(len(p)))
		stats.S3ReadLatency.SampleTimeSince(t1)
	}()
	return s3or.readRange(name, p, s3RangeHeader(off, int64(len(p))))
}

func s3RangeHeader(off, length int64) string {
	lastByte := off + length - 1 // insanely, the HTTP range header specifies ranges inclusively.
	return fmt.Sprintf("%s=%d-%d", s3RangePrefix, off, lastByte)
}

func (s3or *s3ObjectReader) ReadFromEnd(name addr, p []byte, stats *Stats) (n int, err error) {
	// TODO: enable this to use the tableCache. The wrinkle is the tableCache currently just returns a ReaderAt, which doesn't give you the length of the object that backs it, so you can't calculate an offset if all you know is that you want the last N bytes.
	defer func(t1 time.Time) {
		stats.S3BytesPerRead.Sample(uint64(len(p)))
		stats.S3ReadLatency.SampleTimeSince(t1)
	}(time.Now())
	return s3or.readRange(name, p, fmt.Sprintf("%s=-%d", s3RangePrefix, len(p)))
}

func (s3or *s3ObjectReader) readRange(name addr, p []byte, rangeHeader string) (n int, err error) {
	read := func() (int, error) {
		if s3or.readRl != nil {
			s3or.readRl <- struct{}{}
			defer func() {
				<-s3or.readRl
			}()
		}

		input := &s3.GetObjectInput{
			Bucket: aws.String(s3or.bucket),
			Key:    aws.String(name.String()),
			Range:  aws.String(rangeHeader),
		}
		result, err := s3or.s3.GetObject(input)
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
