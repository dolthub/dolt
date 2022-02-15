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
	ti, err := parseTableIndexByCopy(buff)
	require.NoError(t, err)
	tr, err := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)
	require.NoError(t, err)

	hashes := make(hash.HashSet)
	for _, chnk := range testMDChunks {
		hashes.Insert(chnk.Hash())
	}

	reqs := toGetRecords(hashes)
	found := make([]CompressedChunk, 0)

	eg, egCtx := errgroup.WithContext(ctx)
	_, err = tr.getManyCompressed(egCtx, eg, reqs, func(ctx context.Context, c CompressedChunk) { found = append(found, c) }, &Stats{})
	require.NoError(t, err)
	require.NoError(t, eg.Wait())

	// for all the chunks we find, write them using the compressed writer
	tw, err := NewCmpChunkTableWriter("")
	require.NoError(t, err)
	for _, cmpChnk := range found {
		err = tw.AddCmpChunk(cmpChnk)
		require.NoError(t, err)
		err = tw.AddCmpChunk(cmpChnk)
		assert.Equal(t, err, ErrChunkAlreadyWritten)
	}

	id, err := tw.Finish()
	require.NoError(t, err)

	assert.Equal(t, expectedId, id)

	output := bytes.NewBuffer(nil)
	err = tw.Flush(output)
	require.NoError(t, err)

	outputBuff := output.Bytes()
	outputTI, err := parseTableIndexByCopy(outputBuff)
	require.NoError(t, err)
	outputTR, err := newTableReader(outputTI, tableReaderAtFromBytes(buff), fileBlockSize)
	require.NoError(t, err)

	compareContentsOfTables(t, ctx, hashes, tr, outputTR)
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
	_, err := reader.getMany(ctx, eg, reqs, func(ctx context.Context, c *chunks.Chunk) { found = append(found, c) }, &Stats{})
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
