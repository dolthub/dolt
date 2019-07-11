// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

func TestS3TableReaderAt(t *testing.T) {
	s3 := makeFakeS3(t)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h, err := buildTable(chunks)
	assert.NoError(t, err)
	s3.data[h.String()] = tableData

	t.Run("TolerateFailingReads", func(t *testing.T) {
		assert := assert.New(t)

		baseline := s3.getCount
		tra := &s3TableReaderAt{&s3ObjectReader{makeFlakyS3(s3), "bucket", nil, nil, ""}, h}
		scratch := make([]byte, len(tableData))
		_, err := tra.ReadAtWithStats(context.Background(), scratch, 0, &Stats{})
		assert.NoError(err)
		// constructing the table reader should have resulted in 2 reads
		assert.Equal(2, s3.getCount-baseline)
		assert.Equal(tableData, scratch)
	})

	t.Run("WithTableCache", func(t *testing.T) {
		assert := assert.New(t)
		dir := makeTempDir(t)
		defer os.RemoveAll(dir)
		stats := &Stats{}

		tc, err := newFSTableCache(dir, uint64(2*len(tableData)), 4)
		assert.NoError(err)
		tra := &s3TableReaderAt{&s3ObjectReader{s3, "bucket", nil, tc, ""}, h}

		// First, read when table is not yet cached
		scratch := make([]byte, len(tableData))
		baseline := s3.getCount
		_, err = tra.ReadAtWithStats(context.Background(), scratch, 0, stats)
		assert.NoError(err)
		assert.True(s3.getCount > baseline)

		// Cache the table and read again
		err = tc.store(h, bytes.NewReader(tableData), uint64(len(tableData)))
		assert.NoError(err)
		baseline = s3.getCount
		_, err = tra.ReadAtWithStats(context.Background(), scratch, 0, stats)
		assert.NoError(err)
		assert.Zero(s3.getCount - baseline)
	})
}

func TestS3TableReaderAtNamespace(t *testing.T) {
	assert := assert.New(t)

	s3 := makeFakeS3(t)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	ns := "a-prefix-here"

	tableData, h, err := buildTable(chunks)
	assert.NoError(err)
	s3.data["a-prefix-here/"+h.String()] = tableData

	tra := &s3TableReaderAt{&s3ObjectReader{s3, "bucket", nil, nil, ns}, h}
	scratch := make([]byte, len(tableData))
	_, err = tra.ReadAtWithStats(context.Background(), scratch, 0, &Stats{})
	assert.NoError(err)
	assert.Equal(tableData, scratch)
}

type flakyS3 struct {
	s3svc
	alreadyFailed map[string]struct{}
}

func makeFlakyS3(svc s3svc) *flakyS3 {
	return &flakyS3{svc, map[string]struct{}{}}
}

func (fs3 *flakyS3) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	output, err := fs3.s3svc.GetObjectWithContext(ctx, input)

	if err != nil {
		return nil, err
	}

	if _, ok := fs3.alreadyFailed[*input.Key]; !ok {
		fs3.alreadyFailed[*input.Key] = struct{}{}
		output.Body = ioutil.NopCloser(resettingReader{})
	}

	return output, nil
}

type resettingReader struct{}

func (rr resettingReader) Read(p []byte) (n int, err error) {
	return 0, &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}
}
