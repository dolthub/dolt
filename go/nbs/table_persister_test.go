// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestPlanCompaction(t *testing.T) {
	assert := assert.New(t)
	tableContents := [][][]byte{
		{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")},
		{[]byte("red"), []byte("blue")},
		{[]byte("solo")},
	}

	var sources chunkSources
	var dataLens []uint64
	var totalUnc uint64
	for _, content := range tableContents {
		for _, chnk := range content {
			totalUnc += uint64(len(chnk))
		}
		data, name := buildTable(content)
		src := chunkSourceAdapter{newTableReader(parseTableIndex(data), tableReaderAtFromBytes(data), fileBlockSize), name}
		dataLens = append(dataLens, uint64(len(data))-indexSize(src.count())-footerSize)
		sources = append(sources, src)
	}

	plan := planConjoin(sources, &Stats{})

	var totalChunks uint32
	for i, src := range sources {
		assert.Equal(dataLens[i], plan.sources[i].dataLen)
		totalChunks += src.count()
	}

	idx := parseTableIndex(plan.mergedIndex)

	assert.Equal(totalChunks, idx.chunkCount)
	assert.Equal(totalUnc, idx.totalUncompressedData)

	tr := newTableReader(idx, tableReaderAtFromBytes(nil), fileBlockSize)
	for _, content := range tableContents {
		assertChunksInReader(content, tr, assert)
	}
}
