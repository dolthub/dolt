// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		data, name, err := buildTable(content)
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(data)
		require.NoError(t, err)
		tr, err := newTableReader(ti, tableReaderAtFromBytes(data), fileBlockSize)
		require.NoError(t, err)
		src := chunkSourceAdapter{tr, name}
		dataLens = append(dataLens, uint64(len(data))-indexSize(mustUint32(src.count()))-footerSize)
		sources = append(sources, src)
	}

	plan, err := planConjoin(sources, &Stats{})
	require.NoError(t, err)

	var totalChunks uint32
	for i, src := range sources {
		assert.Equal(dataLens[i], plan.sources.sws[i].dataLen)
		totalChunks += mustUint32(src.count())
	}

	idx, err := parseTableIndex(plan.mergedIndex)
	require.NoError(t, err)

	assert.Equal(totalChunks, idx.chunkCount)
	assert.Equal(totalUnc, idx.totalUncompressedData)

	tr, err := newTableReader(idx, tableReaderAtFromBytes(nil), fileBlockSize)
	require.NoError(t, err)
	for _, content := range tableContents {
		assertChunksInReader(content, tr, assert)
	}
}
