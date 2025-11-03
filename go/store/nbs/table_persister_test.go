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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
)

func TestPlanCompaction(t *testing.T) {
	ctx := t.Context()
	assert := assert.New(t)
	tableContents := [][][]byte{
		{[]byte("hello2"), []byte("goodbye2"), []byte("badbye2")},
		{[]byte("red"), []byte("blue")},
		{[]byte("solo")},
	}

	q := NewUnlimitedMemQuotaProvider()
	var sources chunkSources
	var dataLens []uint64
	var totalUnc uint64
	for _, content := range tableContents {
		for _, chnk := range content {
			totalUnc += uint64(len(chnk))
		}
		data, name, err := buildTable(content)
		require.NoError(t, err)
		ti, err := parseTableIndexByCopy(ctx, data, q)
		require.NoError(t, err)
		tr, err := newTableReader(ctx, ti, tableReaderAtFromBytes(data), fileBlockSize)
		require.NoError(t, err)
		src := chunkSourceAdapter{tr, name}
		t.Cleanup(func() { src.close() })
		dataLens = append(dataLens, uint64(len(data))-indexSize(mustUint32(src.count()))-footerSize)
		sources = append(sources, src)
	}

	plan, err := planRangeCopyConjoin(ctx, sources, q, &Stats{})
	require.NoError(t, err)
	defer plan.closer()

	var totalChunks uint32
	for i, src := range sources {
		assert.Equal(dataLens[i], plan.sources.sws[i].dataLen)
		totalChunks += mustUint32(src.count())
	}

	idx, err := parseTableIndexByCopy(ctx, plan.mergedIndex, q)
	require.NoError(t, err)

	assert.Equal(totalChunks, idx.chunkCount())
	assert.Equal(totalUnc, idx.totalUncompressedData())

	tr, err := newTableReader(ctx, idx, tableReaderAtFromBytes(nil), fileBlockSize)
	require.NoError(t, err)
	defer tr.close()
	for _, content := range tableContents {
		assertChunksInReader(content, tr, assert)
	}
}

func TestPlanRangeCopyConjoin(t *testing.T) {
	t.Run("Quota", func(t *testing.T) {
		type testCase struct {
			name string
			mode testConjoinMode
		}
		cases := []testCase{{
			name: "WithArchives",
			mode: conjoinModeArchive,
		}, {
			name: "WitouthArchives",
			mode: conjoinModeTable,
		}}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var beforesize uint64
				var successsize uint64
				t.Run("Success", func(t *testing.T) {
					q := NewUnlimitedMemQuotaProvider()
					ctx := t.Context()
					persister := &blobstorePersister{
						bs:        blobstore.NewInMemoryBlobstore(""),
						blockSize: 4096,
						q:         q,
					}
					srcs := makeTestSrcs(t, []uint32{1024, 1024, 1024, 1024}, persister, tc.mode)
					beforesize = q.Usage()
					plan, err := planRangeCopyConjoin(ctx, srcs, q, &Stats{})
					require.NoError(t, err)
					t.Cleanup(plan.closer)
					successsize = q.Usage()
					require.Greater(t, q.Usage(), beforesize)
					plan.closer()
					require.Equal(t, beforesize, q.Usage())
				})
				t.Run("Failure", func(t *testing.T) {
					for i := beforesize + 1024; i < successsize; i += 1024 {
						t.Run(strconv.Itoa(int(i)), func(t *testing.T) {
							q := &errorQuota{NewUnlimitedMemQuotaProvider(), int(i)}
							ctx := t.Context()
							persister := &blobstorePersister{
								bs:        blobstore.NewInMemoryBlobstore(""),
								blockSize: 4096,
								q:         q,
							}
							srcs := makeTestSrcs(t, []uint32{1024, 1024, 1024, 1024}, persister, tc.mode)
							_, err := planRangeCopyConjoin(ctx, srcs, q, &Stats{})
							require.Error(t, err)
							require.Equal(t, beforesize, q.Usage())
						})
					}
				})
			})
		}
	})
}
