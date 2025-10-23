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
	"crypto/rand"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestCompressedChunkIsEmpty(t *testing.T) {
	if !EmptyCompressedChunk.IsEmpty() {
		t.Fatal("EmptyCompressedChunkIsEmpty() should equal true.")
	}
	if !(CompressedChunk{}).IsEmpty() {
		t.Fatal("CompressedChunk{}.IsEmpty() should equal true.")
	}
}

func TestCanReadAhead(t *testing.T) {
	type expected struct {
		end uint64
		can bool
	}
	type testCase struct {
		rec       offsetRec
		start     uint64
		end       uint64
		blockSize uint64
		ex        expected
	}
	for _, c := range []testCase{
		testCase{offsetRec{offset: 8191, length: 2048}, 0, 4096, 4096, expected{end: 10239, can: true}},
		testCase{offsetRec{offset: 8191, length: 2048}, 0, 4096, 2048, expected{end: 4096, can: false}},
		testCase{offsetRec{offset: 2048, length: 2048}, 0, 4096, 2048, expected{end: 4096, can: true}},
		testCase{offsetRec{offset: (1 << 27), length: 2048}, 0, 128 * 1024 * 1024, 4096, expected{end: 134217728, can: false}},
	} {
		end, can := canReadAhead(c.rec, c.start, c.end, c.blockSize)
		assert.Equal(t, c.ex.end, end)
		assert.Equal(t, c.ex.can, can)
	}
}

func TestTableReaderIndexQuota(t *testing.T) {
	// Write a simple archive file which has non-sense chunks which claim to be snappy encoded.
	dir := t.TempDir()
	writer, err := NewCmpChunkTableWriter(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		writer.Remove()
	})

	var h hash.Hash
	_, err = io.ReadFull(rand.Reader, h[:])
	tableFilePath := filepath.Join(dir, h.String())
	var bytes [1024]byte
	count := 1024
	for i := 0; i < count; i++ {
		_, err := io.ReadFull(rand.Reader, bytes[:])
		require.NoError(t, err)
		chunk := chunks.NewChunk(bytes[:])
		cmpChunk := ChunkToCompressedChunk(chunk)
		_, err = writer.AddChunk(cmpChunk)
		require.NoError(t, err)
	}
	_, _, err = writer.Finish()
	require.NoError(t, err)
	err = writer.FlushToFile(tableFilePath)
	require.NoError(t, err)

	t.Run("Success", func(t *testing.T) {
		ctx := t.Context()
		q := NewUnlimitedMemQuotaProvider()
		assert.Equal(t, uint64(0), q.Usage())
		// stats := &Stats{}
		reader, err := nomsFileTableReader(ctx, tableFilePath, h, uint32(count), q)
		require.NoError(t, err)
		// Immediately after opening the quota acocunts for in memory index.
		expectedQuotaUsage :=
			indexSize(uint32(count)) + // The bytes for the index
				footerSize + // The footer
				uint64(uint64Size*count) + // The bigendian interpreted prefixes
				uint64(offsetSize*(count/2)) // The offset bytes, for half the chunks. The other half reuses buff.
		assert.Equal(t, expectedQuotaUsage, q.Usage())
		// Cloning doesn't change quota usage.
		cloned, err := reader.clone()
		require.NoError(t, err)
		assert.Equal(t, expectedQuotaUsage, q.Usage())
		// Closing the original keeps the quota while cloned is around.
		require.NoError(t, reader.close())
		assert.Equal(t, expectedQuotaUsage, q.Usage())
		// Closing the clone does release the quota.
		require.NoError(t, cloned.close())
		assert.Equal(t, uint64(0), q.Usage())
	})
}
