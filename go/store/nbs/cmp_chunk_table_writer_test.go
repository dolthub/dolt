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

package nbs

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestCmpChunkTableWriter(t *testing.T) {
	// Put some chunks in a table file and get the buffer back which contains the table file data
	ctx := context.Background()

	expectedId, buff, err := WriteChunks(testMDChunks)
	require.NoError(t, err)

	// Setup a TableReader to read compressed chunks out of
	ti, err := parseTableIndexByCopy(ctx, buff, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
	require.NoError(t, err)
	defer tr.close()

	hashes := make(hash.HashSet)
	for _, chnk := range testMDChunks {
		hashes.Insert(chnk.Hash())
	}

	reqs := toGetRecords(hashes)
	found := make([]CompressedChunk, 0)

	eg, egCtx := errgroup.WithContext(ctx)
	_, _, err = tr.getManyCompressed(egCtx, eg, reqs, func(ctx context.Context, c CompressedChunk) { found = append(found, c) }, nil, &Stats{})
	require.NoError(t, err)
	require.NoError(t, eg.Wait())

	// for all the chunks we find, write them using the compressed writer
	tw, err := NewCmpChunkTableWriter(t.TempDir())
	require.NoError(t, err)
	for _, cmpChnk := range found {
		err = tw.AddCmpChunk(cmpChnk)
		require.NoError(t, err)
	}

	id, err := tw.Finish()
	require.NoError(t, err)

	t.Run("ErrDuplicateChunkWritten", func(t *testing.T) {
		tw, err := NewCmpChunkTableWriter(t.TempDir())
		require.NoError(t, err)
		for _, cmpChnk := range found {
			err = tw.AddCmpChunk(cmpChnk)
			require.NoError(t, err)
			err = tw.AddCmpChunk(cmpChnk)
			require.NoError(t, err)
		}
		_, err = tw.Finish()
		require.Error(t, err, ErrDuplicateChunkWritten)
	})

	assert.Equal(t, expectedId, id)

	output := bytes.NewBuffer(nil)
	err = tw.Flush(output)
	require.NoError(t, err)

	outputBuff := output.Bytes()
	outputTI, err := parseTableIndexByCopy(ctx, outputBuff, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	outputTR, err := newTableReader(outputTI, tableReaderAtFromBytes(buff), fileBlockSize)
	require.NoError(t, err)
	defer outputTR.close()

	compareContentsOfTables(t, ctx, hashes, tr, outputTR)
}

func TestCmpChunkTableWriterGhostChunk(t *testing.T) {
	tw, err := NewCmpChunkTableWriter(t.TempDir())
	require.NoError(t, err)
	require.Error(t, tw.AddCmpChunk(NewGhostCompressedChunk(hash.Parse("6af71afc2ea0hmp4olev0vp9q1q5gvb1"))))
}

func TestContainsDuplicates(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		require.False(t, containsDuplicates(prefixIndexSlice{}))
	})
	t.Run("ManyUniqueMatchingPrefixes", func(t *testing.T) {
		var recs prefixIndexSlice
		for i := 0; i < 256; i++ {
			var rec prefixIndexRec
			rec.addr[19] = byte(i)
			recs = append(recs, rec)
		}
		sort.Sort(recs)
		require.False(t, containsDuplicates(recs))
	})
	t.Run("OneDuplicate", func(t *testing.T) {
		var recs prefixIndexSlice
		for i := 0; i < 256; i++ {
			var rec prefixIndexRec
			rec.addr[19] = byte(i)
			recs = append(recs, rec)
		}
		{
			var rec prefixIndexRec
			rec.addr[19] = byte(128)
			recs = append(recs, rec)
		}
		sort.Sort(recs)
		require.True(t, containsDuplicates(recs))
	})
}

func compareContentsOfTables(t *testing.T, ctx context.Context, hashes hash.HashSet, expectedRd, actualRd tableReader) {
	expected, err := readAllChunks(ctx, hashes, expectedRd)
	require.NoError(t, err)
	actual, err := readAllChunks(ctx, hashes, actualRd)
	require.NoError(t, err)

	assert.Equal(t, len(expected), len(actual))
	assert.Equal(t, expected, actual)
}

func readAllChunks(ctx context.Context, hashes hash.HashSet, reader tableReader) (map[hash.Hash][]byte, error) {
	reqs := toGetRecords(hashes)
	found := make([]*chunks.Chunk, 0)
	eg, ctx := errgroup.WithContext(ctx)
	_, _, err := reader.getMany(ctx, eg, reqs, func(ctx context.Context, c *chunks.Chunk) { found = append(found, c) }, nil, &Stats{})
	if err != nil {
		return nil, err
	}
	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	hashToData := make(map[hash.Hash][]byte)
	for _, c := range found {
		hashToData[c.Hash()] = c.Data()
	}

	return hashToData, nil
}
