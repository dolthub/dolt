// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"io/ioutil"
	"net"
	"os"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/attic-labs/testify/assert"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3TableReader(t *testing.T) {
	assert := assert.New(t)
	s3 := makeFakeS3(assert)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h := buildTable(chunks)
	s3.data[h.String()] = tableData

	trc := newS3TableReader(s3, "bucket", h, uint32(len(chunks)), nil, nil)
	defer trc.close()
	assertChunksInReader(chunks, trc, assert)
}

func TestS3TableReaderIndexCache(t *testing.T) {
	assert := assert.New(t)
	s3 := makeFakeS3(assert)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h := buildTable(chunks)

	s3.data[h.String()] = tableData

	index := parseTableIndex(tableData)
	cache := newIndexCache(1024)
	cache.put(h, index)

	trc := newS3TableReader(s3, "bucket", h, uint32(len(chunks)), cache, nil)

	assert.Equal(0, s3.getCount) // constructing the table shouldn't have resulted in any reads

	defer trc.close()
	assertChunksInReader(chunks, trc, assert)
}

func TestS3TableReaderFails(t *testing.T) {
	assert := assert.New(t)
	fake := makeFakeS3(assert)

	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}

	tableData, h := buildTable(chunks)

	fake.data[h.String()] = tableData

	trc := newS3TableReader(makeFlakyS3(fake), "bucket", h, uint32(len(chunks)), nil, nil)
	assert.Equal(2, fake.getCount) // constructing the table should have resulted in 2 reads

	defer trc.close()
	assertChunksInReader(chunks, trc, assert)
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
