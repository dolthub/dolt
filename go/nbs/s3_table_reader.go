// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/attic-labs/noms/go/d"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
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
}

type s3svc interface {
	AbortMultipartUpload(input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)
	CreateMultipartUpload(input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(input *s3.UploadPartInput) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

func newS3TableReader(s3 s3svc, bucket string, h addr, chunkCount uint32, indexCache *indexCache, readRl chan struct{}) chunkSource {
	source := &s3TableReader{s3: s3, bucket: bucket, h: h, readRl: readRl}

	var index tableIndex
	found := false
	if indexCache != nil {
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

func (s3tr *s3TableReader) close() error {
	return nil
}

func (s3tr *s3TableReader) hash() addr {
	return s3tr.h
}

func (s3tr *s3TableReader) ReadAt(p []byte, off int64) (n int, err error) {
	end := off + int64(len(p)) - 1 // insanely, the HTTP range header specifies ranges inclusively.
	rangeHeader := fmt.Sprintf("%s=%d-%d", s3RangePrefix, off, end)
	return s3tr.readRange(p, rangeHeader)
}

func (s3tr *s3TableReader) readRange(p []byte, rangeHeader string) (n int, err error) {
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
	// TODO: take out this running of ss once BUG 3255 is fixed
	shouldLog := make(chan struct{})
	defer close(shouldLog)
	ss, logFinished := newBg(shouldLog, "/usr/sbin/ss", "-ntp")
	go ss.Run("Couldn't run ss to record network connections:")

	// TODO: go back to just calling GetObject once BUG 3255 is fixed
	result, reqID, reqID2, err := func() (*s3.GetObjectOutput, string, string, error) {
		if impl, ok := s3tr.s3.(*s3.S3); ok {
			req, result := impl.GetObjectRequest(input)
			err := req.Send()
			return result, req.RequestID, req.HTTPResponse.Header.Get("x-amz-id-2"), err
		}
		result, err := s3tr.s3.GetObject(input)
		return result, "FAKE", "FAKE", err
	}()
	d.PanicIfError(err)
	d.PanicIfFalse(*result.ContentLength == int64(len(p)))

	n, err = io.ReadFull(result.Body, p)

	if err != nil {
		shouldLog <- struct{}{}
		<-logFinished
		d.Chk.Fail("Failed ranged read from S3\n", "req %s\nreq %s\n%s\nerror: %v", reqID, reqID2, input.GoString(), err)
	}

	return n, err
}

type bgCmd struct {
	cmd         *exec.Cmd
	startLog    <-chan struct{}
	logFinished chan<- struct{}
}

func newBg(startLog <-chan struct{}, path string, args ...string) (cmd *bgCmd, finished <-chan struct{}) {
	f := make(chan struct{})
	cmd = &bgCmd{exec.Command(path, args...), startLog, f}
	return cmd, f
}

func (w *bgCmd) Run(msg string) {
	defer close(w.logFinished)
	out, err := w.cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintln(os.Stderr, msg, err)
		return
	}
	if _, log := <-w.startLog; log {
		fmt.Fprintln(os.Stderr, out)
	}
}
