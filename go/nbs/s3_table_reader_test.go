// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"testing"

	"github.com/attic-labs/testify/assert"
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

	trc := newS3TableReader(s3, "bucket", h, uint32(len(chunks)), nil)
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
	cache := newS3IndexCache(1024)
	cache.put(h, index)

	trc := newS3TableReader(s3, "bucket", h, uint32(len(chunks)), cache)

	assert.Equal(0, s3.getCount) // constructing the table shouldn't have resulted in any reads

	defer trc.close()
	assertChunksInReader(chunks, trc, assert)
}
