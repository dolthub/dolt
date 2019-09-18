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
	"github.com/stretchr/testify/suite"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/chunks"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

func TestBlockBufferTableSink(t *testing.T) {
	suite.Run(t, &TableSinkSuite{sink: NewBlockBufferTableSink(128)})
}

func TestFixedBufferTableSink(t *testing.T) {
	suite.Run(t, &TableSinkSuite{sink: NewFixedBufferTableSink(make([]byte, 32*1024))})
}

type TableSinkSuite struct {
	sink ByteSink
	t    *testing.T
}

func (suite *TableSinkSuite) SetT(t *testing.T) {
	suite.t = t
}

func (suite *TableSinkSuite) T() *testing.T {
	return suite.t
}

func (suite *TableSinkSuite) TestWrite() {
	data := make([]byte, 64)
	for i := 0; i < 64; i++ {
		data[i] = byte(i)
	}

	for i := 0; i < 32; i++ {
		_, err := suite.sink.Write(data)
		assert.NoError(suite.t, err)
	}
}

func TestCmpChunkTableWriter(t *testing.T) {
	// Put some chunks in a table file and get the buffer back which cotains the table file data
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
		tr.getManyCompressed(ctx, reqs, found, wg, ae, &Stats{})
		wg.Wait()
	}()

	// for all the chunks we find, write them using the compressed writer
	tw := NewCmpChunkTableWriter()
	for cmpChnk := range found {
		err = tw.AddCmpChunk(cmpChnk)
		require.NoError(t, err)
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
		reader.getMany(ctx, reqs, found, wg, ae, &Stats{})
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
