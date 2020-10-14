// Copyright 2019 Liquidata, Inc.
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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestCmpChunkTableWriter(t *testing.T) {
	// Put some chunks in a table file and get the buffer back which contains the table file data
	ctx := context.Background()

	expectedId, buff, err := WriteChunks(testMDChunks)
	require.NoError(t, err)

	// Setup a TableReader to read compressed chunks out of
	ti, err := parseTableIndex(buff)
	require.NoError(t, err)
	tr := newTableReader(ti, tableReaderAtFromBytes(buff), fileBlockSize)

	hashes := make(hash.HashSet)
	for _, chnk := range testMDChunks {
		hashes.Insert(chnk.Hash())
	}

	ae := atomicerr.New()
	wg := &sync.WaitGroup{}
	reqs := toGetRecords(hashes)
	found := make(chan CompressedChunk, 128)

	go func() {
		defer close(found)
		tr.getManyCompressed(ctx, reqs, func (c CompressedChunk) { found<-c }, wg, ae, &Stats{})
		wg.Wait()
	}()

	// for all the chunks we find, write them using the compressed writer
	tw, err := NewCmpChunkTableWriter("")
	require.NoError(t, err)
	for cmpChnk := range found {
		err = tw.AddCmpChunk(cmpChnk)
		require.NoError(t, err)
		err = tw.AddCmpChunk(cmpChnk)
		assert.Equal(t, err, ErrChunkAlreadyWritten)
	}

	require.NoError(t, ae.Get())

	id, err := tw.Finish()
	require.NoError(t, err)

	assert.Equal(t, expectedId, id)

	output := bytes.NewBuffer(nil)
	err = tw.Flush(output)
	require.NoError(t, err)

	outputBuff := output.Bytes()
	outputTI, err := parseTableIndex(outputBuff)
	require.NoError(t, err)
	outputTR := newTableReader(outputTI, tableReaderAtFromBytes(buff), fileBlockSize)

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
	wg := &sync.WaitGroup{}
	ae := atomicerr.New()
	reqs := toGetRecords(hashes)
	found := make(chan *chunks.Chunk, 128)

	go func() {
		defer close(found)
		reader.getMany(ctx, reqs, func (c *chunks.Chunk) { found<-c }, wg, ae, &Stats{})
		wg.Wait()
	}()

	hashToData := make(map[hash.Hash][]byte)
	for c := range found {
		hashToData[c.Hash()] = c.Data()
	}

	if err := ae.Get(); err != nil {
		return nil, err
	}

	return hashToData, nil
}
