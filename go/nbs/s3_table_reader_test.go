// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3TableReader(t *testing.T) {
	s3 := makeFakeS3(t)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h := buildTable(chunks)
	s3.data[h.String()] = tableData

	t.Run("NoIndexCache", func(t *testing.T) {
		trc := newS3TableReader(s3, "bucket", h, uint32(len(chunks)), nil, nil, nil)
		assertChunksInReader(chunks, trc, assert.New(t))
	})

	t.Run("WithIndexCache", func(t *testing.T) {
		assert := assert.New(t)
		index := parseTableIndex(tableData)
		cache := newIndexCache(1024)
		cache.put(h, index)

		baseline := s3.getCount
		trc := newS3TableReader(s3, "bucket", h, uint32(len(chunks)), cache, nil, nil)

		// constructing the table reader shouldn't have resulted in any reads
		assert.Zero(s3.getCount - baseline)
		assertChunksInReader(chunks, trc, assert)
	})

	t.Run("TolerateFailingReads", func(t *testing.T) {
		assert := assert.New(t)

		baseline := s3.getCount
		trc := newS3TableReader(makeFlakyS3(s3), "bucket", h, uint32(len(chunks)), nil, nil, nil)
		// constructing the table reader should have resulted in 2 reads
		assert.Equal(2, s3.getCount-baseline)
		assertChunksInReader(chunks, trc, assert)
	})

	t.Run("WithTableCache", func(t *testing.T) {
		assert := assert.New(t)
		dir := makeTempDir(t)
		defer os.RemoveAll(dir)
		stats := &Stats{}

		tc := newFSTableCache(dir, uint64(2*len(tableData)), 4)
		trc := newS3TableReader(s3, "bucket", h, uint32(len(chunks)), nil, nil, tc)
		tra := trc.(tableReaderAt)

		// First, read when table is not yet cached
		scratch := make([]byte, len(tableData))
		baseline := s3.getCount
		_, err := tra.ReadAtWithStats(scratch, 0, stats)
		assert.NoError(err)
		assert.True(s3.getCount > baseline)

		// Cache the table and read again
		tc.store(h, bytes.NewReader(tableData), uint64(len(tableData)))
		baseline = s3.getCount
		_, err = tra.ReadAtWithStats(scratch, 0, stats)
		assert.NoError(err)
		assert.Zero(s3.getCount - baseline)
	})
}

type flakyS3 struct {
	s3svc
	alreadyFailed map[string]struct{}
}

func makeFlakyS3(svc s3svc) *flakyS3 {
	return &flakyS3{svc, map[string]struct{}{}}
}

func (fs3 *flakyS3) GetObject(input *s3.GetObjectInput) (output *s3.GetObjectOutput, err error) {
	output, err = fs3.s3svc.GetObject(input)
	if _, ok := fs3.alreadyFailed[*input.Key]; !ok {
		fs3.alreadyFailed[*input.Key] = struct{}{}
		output.Body = ioutil.NopCloser(resettingReader{})
	}
	return
}

type resettingReader struct{}

func (rr resettingReader) Read(p []byte) (n int, err error) {
	return 0, &net.OpError{Op: "read", Net: "tcp", Err: &os.SyscallError{Syscall: "read", Err: unix.ECONNRESET}}
}
