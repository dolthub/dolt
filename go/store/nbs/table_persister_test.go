// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"github.com/liquidata-inc/ld/dolt/go/store/must"
	"testing"

	"github.com/stretchr/testify/assert"
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
		ti, err := parseTableIndex(data)
		assert.NoError(err)
		src := chunkSourceAdapter{newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize), name}
		dataLens = append(dataLens, uint64(len(data))-indexSize(must.Uint32(src.count()))-footerSize)
		sources = append(sources, src)
	}

	plan := planConjoin(sources, &Stats{})

	var totalChunks uint32
	for i, src := range sources {
		assert.Equal(dataLens[i], plan.sources[i].dataLen)
		totalChunks += must.Uint32(src.count())
	}

	idx, err := parseTableIndex(plan.mergedIndex)
	assert.NoError(err)

	assert.Equal(totalChunks, idx.chunkCount)
	assert.Equal(totalUnc, idx.totalUncompressedData)

	tr := newTableReader(idx, tableReaderAtFromBytes(nil), fileBlockSize)
	for _, content := range tableContents {
		assertChunksInReader(content, tr, assert)
	}
}
