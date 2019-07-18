// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSChunkSource(t *testing.T) {
	chunks := [][]byte{
		[]byte("hello2"),
		[]byte("goodbye2"),
		[]byte("badbye2"),
	}
	tableData, h, err := buildTable(chunks)
	assert.NoError(t, err)

	s3 := makeFakeS3(t)
	ddb := makeFakeDDB(t)

	s3or := &s3ObjectReader{s3, "bucket", nil, nil, ""}
	dts := &ddbTableStore{ddb, "table", nil, nil}

	makeSrc := func(chunkMax int, ic *indexCache) chunkSource {
		cs, err := newAWSChunkSource(
			context.Background(),
			dts,
			s3or,
			awsLimits{itemMax: maxDynamoItemSize, chunkMax: uint32(chunkMax)},
			h,
			uint32(len(chunks)),
			ic,
			&Stats{},
		)

		assert.NoError(t, err)

		return cs
	}

	t.Run("Dynamo", func(t *testing.T) {
		ddb.putData(fmtTableName(h), tableData)

		t.Run("NoIndexCache", func(t *testing.T) {
			src := makeSrc(len(chunks)+1, nil)
			assertChunksInReader(chunks, src, assert.New(t))
		})

		t.Run("WithIndexCache", func(t *testing.T) {
			assert := assert.New(t)
			index, err := parseTableIndex(tableData)
			assert.NoError(err)
			cache := newIndexCache(1024)
			cache.put(h, index)

			baseline := ddb.numGets
			src := makeSrc(len(chunks)+1, cache)

			// constructing the table reader shouldn't have resulted in any reads
			assert.Zero(ddb.numGets - baseline)
			assertChunksInReader(chunks, src, assert)
		})
	})

	t.Run("S3", func(t *testing.T) {
		s3.data[h.String()] = tableData

		t.Run("NoIndexCache", func(t *testing.T) {
			src := makeSrc(len(chunks)-1, nil)
			assertChunksInReader(chunks, src, assert.New(t))
		})

		t.Run("WithIndexCache", func(t *testing.T) {
			assert := assert.New(t)
			index, err := parseTableIndex(tableData)
			assert.NoError(err)
			cache := newIndexCache(1024)
			cache.put(h, index)

			baseline := s3.getCount
			src := makeSrc(len(chunks)-1, cache)

			// constructing the table reader shouldn't have resulted in any reads
			assert.Zero(s3.getCount - baseline)
			assertChunksInReader(chunks, src, assert)
		})
	})
}
