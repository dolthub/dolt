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
	"encoding/binary"
	"os"
	"path/filepath"
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

	expectedId, buff, _, err := WriteChunks(testMDChunks)
	require.NoError(t, err)

	// Setup a TableReader to read compressed chunks out of
	ti, err := parseTableIndexByCopy(ctx, buff, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	tr, err := newTableReader(t.Context(), ti, tableReaderAtFromBytes(buff), fileBlockSize)
	require.NoError(t, err)
	defer tr.close()

	hashes := make(hash.HashSet)
	for _, chnk := range testMDChunks {
		hashes.Insert(chnk.Hash())
	}

	reqs := toGetRecords(hashes)
	found := make([]ToChunker, 0)

	eg, egCtx := errgroup.WithContext(ctx)
	_, _, err = tr.getManyCompressed(egCtx, eg, reqs, func(ctx context.Context, c ToChunker) { found = append(found, c) }, nil, &Stats{})
	require.NoError(t, err)
	require.NoError(t, eg.Wait())

	// for all the chunks we find, write them using the compressed writer
	tw, err := NewCmpChunkTableWriter("")
	require.NoError(t, err)
	for _, cmpChnk := range found {
		_, err = tw.AddChunk(cmpChnk)
		require.NoError(t, err)
	}

	_, id, err := tw.Finish()
	require.NoError(t, err)

	t.Run("ErrDuplicateChunkWritten", func(t *testing.T) {
		tw, err := NewCmpChunkTableWriter("")
		require.NoError(t, err)
		for _, cmpChnk := range found {
			_, err = tw.AddChunk(cmpChnk)
			require.NoError(t, err)
			_, err = tw.AddChunk(cmpChnk)
			require.NoError(t, err)
		}
		_, _, err = tw.Finish()
		require.Error(t, err, ErrDuplicateChunkWritten)
	})

	assert.Equal(t, expectedId, id)

	output := bytes.NewBuffer(nil)
	err = tw.Flush(output)
	require.NoError(t, err)

	outputBuff := output.Bytes()
	outputTI, err := parseTableIndexByCopy(ctx, outputBuff, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	outputTR, err := newTableReader(t.Context(), outputTI, tableReaderAtFromBytes(buff), fileBlockSize)
	require.NoError(t, err)
	defer outputTR.close()

	compareContentsOfTables(t, ctx, hashes, tr, outputTR)
}

func TestCmpChunkTableWriterGhostChunk(t *testing.T) {
	tw, err := NewCmpChunkTableWriter("")
	require.NoError(t, err)
	_, err = tw.AddChunk(NewGhostCompressedChunk(hash.Parse("6af71afc2ea0hmp4olev0vp9q1q5gvb1")))
	require.Error(t, err)
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

// TestCmpChunkTableWriterLargeIndex is a regression test for dolt#11747, where
// a GC output file was written missing the tail of its index. Finish writes the
// whole index in one Write, and the sink dropped the block that write left
// behind whenever it had no spare capacity.
func TestCmpChunkTableWriterLargeIndex(t *testing.T) {
	ctx := context.Background()

	// The runtime's allocation granularity for large objects. Only used to
	// search for a triggering chunk count; the probe below is what actually
	// ties this test to the bug.
	const goPageSize = 8192
	const contentLen = 16
	const indexEntrySize = prefixTupleSize + lengthSize + hash.SuffixLen

	// Content this short always encodes as a single snappy literal, so every
	// chunk contributes the same number of bytes.
	perChunkData := len(ChunkToCompressedChunk(chunks.NewChunk(make([]byte, contentLen))).FullCompressedChunk)

	// Above 2*blockSize, append() asks for a capacity equal to the leftover's
	// own length, which the runtime rounds up to a page and no further --- so
	// a page-aligned leftover gets a block that is full on arrival. blockSize
	// is itself page aligned, so that happens exactly when the file bar its
	// footer is. Walk up to a qualifying count; a multiple of goPageSize
	// always works, so this terminates.
	minLeftover := 2*defaultTableSinkBlockSize + goPageSize
	chunkCount := (minLeftover + defaultTableSinkBlockSize + indexEntrySize - 1) / indexEntrySize
	for (chunkCount*(perChunkData+indexEntrySize))%goPageSize != 0 {
		chunkCount++
	}

	dataLen := chunkCount * perChunkData
	indexLen := chunkCount * indexEntrySize
	// Room left in the block the sink holds when the index write arrives, or
	// zero if the chunk data ended flush and the sink holds no block.
	remaining := (defaultTableSinkBlockSize - dataLen%defaultTableSinkBlockSize) % defaultTableSinkBlockSize
	leftover := indexLen - remaining
	require.Greater(t, leftover, 2*defaultTableSinkBlockSize)

	// Ask the runtime rather than assume: the leftover has to land on a block
	// that comes back exactly full.
	probe := append(make([]byte, 0, defaultTableSinkBlockSize), make([]byte, leftover)...)
	require.Equal(t, leftover, cap(probe),
		"a %d byte leftover does not fill its block exactly on this runtime, so this test cannot reproduce dolt#11747", leftover)

	tw, err := NewCmpChunkTableWriter("")
	require.NoError(t, err)

	content := make([]byte, contentLen)
	hashes := make([]hash.Hash, 0, chunkCount)
	var wroteData int
	for i := 0; i < chunkCount; i++ {
		binary.BigEndian.PutUint64(content, uint64(i))
		c := chunks.NewChunk(content)
		hashes = append(hashes, c.Hash())
		n, err := tw.AddChunk(ChunkToCompressedChunk(c))
		require.NoError(t, err)
		wroteData += int(n)
	}
	require.Equal(t, chunkCount, tw.ChunkCount())
	// The sizing above assumes every chunk compressed to the same length.
	require.Equal(t, dataLen, wroteData, "chunks did not all compress to %d bytes", perChunkData)

	_, name, err := tw.Finish()
	require.NoError(t, err)
	require.EqualValues(t, dataLen+indexLen+footerSize, tw.FullLength())

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, tw.FlushToFile(path))

	// Every byte the writer accounted for has to be on disk. FullLength is
	// what the persisters report and what push sends, so a short file here is
	// corruption nothing downstream would notice.
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(tw.FullLength()), stat.Size(),
		"table file is %d bytes short of the %d bytes written",
		int64(tw.FullLength())-stat.Size(), tw.FullLength())

	// And the file has to actually parse and serve every chunk back.
	buff, err := os.ReadFile(path)
	require.NoError(t, err)

	ti, err := parseTableIndexByCopy(ctx, buff, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	tr, err := newTableReader(ctx, ti, tableReaderAtFromBytes(buff), fileBlockSize)
	require.NoError(t, err)
	defer tr.close()
	require.EqualValues(t, chunkCount, tr.count())

	// get() reads the prefix map, the suffixes and the lengths, so reading
	// every chunk back covers all three index regions.
	for i, h := range hashes {
		binary.BigEndian.PutUint64(content, uint64(i))
		data, _, err := tr.get(ctx, h, nil, &Stats{})
		require.NoError(t, err)
		if !bytes.Equal(content, data) {
			t.Fatalf("chunk %d (%s) did not read back: got %x, want %x", i, h.String(), data, content)
		}
	}
}
