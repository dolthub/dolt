// Copyright 2022 Dolthub, Inc.
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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type operation struct {
	kind   opKind
	buf    []byte
	readAt int64
}

type opKind byte

const (
	readOp opKind = iota
	writeOp
	flushOp
)

func TestJournalWriter(t *testing.T) {
	tests := []struct {
		name string
		size int
		ops  []operation
	}{
		{
			name: "smoke test",
			size: 16,
		},
		{
			name: "write to empty file",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("lorem")},
				{kind: writeOp, buf: []byte("ipsum")},
			},
		},
		{
			name: "read from non-empty file",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("loremipsum")},
				{kind: flushOp},
				{kind: readOp, buf: []byte("lorem"), readAt: 0},
				{kind: readOp, buf: []byte("ipsum"), readAt: 5},
				{kind: readOp, buf: []byte("loremipsum"), readAt: 0},
			},
		},
		{
			name: "read new writes",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("lorem")},
				{kind: readOp, buf: []byte("lorem"), readAt: 0},
				{kind: writeOp, buf: []byte("ipsum")},
				{kind: readOp, buf: []byte("lorem"), readAt: 0},
				{kind: readOp, buf: []byte("ipsum"), readAt: 5},
			},
		},
		{
			name: "read flushed writes",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("lorem")},
				{kind: flushOp},
				{kind: readOp, buf: []byte("lorem"), readAt: 0},
				{kind: writeOp, buf: []byte("ipsum")},
				{kind: readOp, buf: []byte("ipsum"), readAt: 5},
				{kind: readOp, buf: []byte("lorem"), readAt: 0},
				{kind: flushOp},
			},
		},
		{
			name: "read partially flushed writes",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("lorem")},
				{kind: flushOp},
				{kind: writeOp, buf: []byte("ipsum")},
				{kind: readOp, buf: []byte("loremipsum"), readAt: 0},
			},
		},
		{
			name: "successive writes trigger buffer flush",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("lorem")},
				{kind: readOp, buf: []byte("lorem"), readAt: 0},
				{kind: writeOp, buf: []byte("ipsum")},
				{kind: readOp, buf: []byte("ipsum"), readAt: 5},
				{kind: writeOp, buf: []byte("dolor")},
				{kind: readOp, buf: []byte("dolor"), readAt: 10},
				{kind: writeOp, buf: []byte("sit")}, // triggers a flush
				{kind: readOp, buf: []byte("sit"), readAt: 15},
				{kind: readOp, buf: []byte("loremipsumdolorsit"), readAt: 0},
				{kind: writeOp, buf: []byte("amet")},
				{kind: readOp, buf: []byte("amet"), readAt: 18},
				{kind: readOp, buf: []byte("loremipsumdolorsitamet"), readAt: 0},
			},
		},
		{
			name: "write larger that buffer",
			size: 8,
			ops: []operation{
				{kind: writeOp, buf: []byte("loremipsum")},
				{kind: flushOp},
				{kind: writeOp, buf: []byte("dolorsitamet")},
				{kind: readOp, buf: []byte("dolorsitamet"), readAt: 10},
				{kind: readOp, buf: []byte("loremipsumdolorsitamet"), readAt: 0},
			},
		},
		{
			name: "flush empty buffer",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("loremipsum")},
				{kind: flushOp},
			},
		},
		{
			name: "double flush write",
			size: 16,
			ops: []operation{
				{kind: writeOp, buf: []byte("loremipsum")},
				{kind: flushOp},
				{kind: writeOp, buf: []byte("dolor")},
				{kind: flushOp},
				{kind: flushOp},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			j, err := createJournalWriter(ctx, newTestFilePath(t))
			require.NotNil(t, j)
			require.NoError(t, err)

			var off int64
			for i, op := range test.ops {
				switch op.kind {
				case readOp:
					act := make([]byte, len(op.buf))
					n, err := j.ReadAt(act, op.readAt)
					assert.NoError(t, err, "operation %d errored", i)
					assert.Equal(t, len(op.buf), n, "operation %d failed", i)
					assert.Equal(t, op.buf, act, "operation %d failed", i)
				case writeOp:
					n, err := j.Write(op.buf)
					assert.NoError(t, err, "operation %d errored", i)
					assert.Equal(t, len(op.buf), n, "operation %d failed", i)
					off += int64(n)
				case flushOp:
					err = j.flush()
					assert.NoError(t, err, "operation %d errored", i)
				default:
					t.Fatal("unknown opKind")
				}
				assert.Equal(t, off, j.offset())
			}
			assert.NoError(t, j.Close())
		})
	}
}

func TestJournalWriterWriteChunk(t *testing.T) {
	ctx := context.Background()
	j, err := createJournalWriter(ctx, newTestFilePath(t))
	require.NotNil(t, j)
	require.NoError(t, err)

	data := randomCompressedChunks(128)
	lookups := make(map[addr]recLookup)

	for a, cc := range data {
		l, err := j.WriteChunk(cc)
		require.NoError(t, err)
		lookups[a] = l
		validateLookup(t, j, l, cc)
	}
	for a, l := range lookups {
		validateLookup(t, j, l, data[a])
	}
	require.NoError(t, j.Close())
}

func TestJournalWriterBootstrap(t *testing.T) {
	ctx := context.Background()
	path := newTestFilePath(t)
	j, err := createJournalWriter(ctx, path)
	require.NotNil(t, j)
	require.NoError(t, err)

	data := randomCompressedChunks(128)
	lookups := make(map[addr]recLookup)
	for a, cc := range data {
		l, err := j.WriteChunk(cc)
		require.NoError(t, err)
		lookups[a] = l
	}
	assert.NoError(t, j.Close())

	j, _, err = openJournalWriter(ctx, path)
	require.NoError(t, err)
	_, source, err := j.ProcessJournal(ctx)
	require.NoError(t, err)

	for a, l := range lookups {
		validateLookup(t, j, l, data[a])
	}
	for a, cc := range data {
		buf, err := source.get(ctx, a, nil)
		require.NoError(t, err)
		ch, err := cc.ToChunk()
		require.NoError(t, err)
		assert.Equal(t, ch.Data(), buf)
	}
	require.NoError(t, j.Close())
}

func TestJournalWriterSyncClose(t *testing.T) {
	ctx := context.Background()
	j, err := createJournalWriter(ctx, newTestFilePath(t))
	require.NotNil(t, j)
	require.NoError(t, err)
	_, _, err = j.ProcessJournal(ctx)
	require.NoError(t, err)

	// close triggers flush
	n, err := j.Write([]byte("sit"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	err = j.Close()
	require.NoError(t, err)
	assert.Equal(t, 0, len(j.buf))
	assert.Equal(t, 3, int(j.off))
}

func TestWriteJournalToTableFile(t *testing.T) {
	ctx := context.Background()
	j, err := createJournalWriter(ctx, newTestFilePath(t))
	require.NotNil(t, j)
	require.NoError(t, err)

	var uncompressed uint64
	data := randomCompressedChunks(16)
	chks := make([][]byte, 0, len(data))
	for _, cc := range data {
		_, err = j.WriteChunk(cc)
		require.NoError(t, err)
		ch, err := cc.ToChunk()
		require.NoError(t, err)
		chks = append(chks, ch.Data())
		uncompressed += uint64(ch.Size())
	}
	require.NoError(t, j.flush())

	rd, err := os.Open(j.path)
	require.NoError(t, err)

	wr := bytes.NewBuffer(nil)
	require.NoError(t, err)
	_, err = writeJournalToTable(ctx, rd, wr)
	require.NoError(t, err)

	expected, _, err := buildTable(chks)
	require.NoError(t, err)
	assert.Equal(t, expected, wr.Bytes())
}

func validateLookup(t *testing.T, j *journalWriter, l recLookup, cc CompressedChunk) {
	b := make([]byte, l.recordLen)
	n, err := j.ReadAt(b, l.journalOff)
	require.NoError(t, err)
	assert.Equal(t, int(l.recordLen), n)
	rec, err := readJournalRecord(b)
	require.NoError(t, err)
	assert.Equal(t, hash.Hash(rec.address), cc.Hash())
	assert.Equal(t, rec.payload, cc.FullCompressedChunk)
}

func newTestFilePath(t *testing.T) string {
	name := fmt.Sprintf("journal%d.log", rand.Intn(65536))
	return filepath.Join(t.TempDir(), name)
}

func randomCompressedChunks(count int) (compressed map[addr]CompressedChunk) {
	compressed = make(map[addr]CompressedChunk)
	var buf []byte
	for i := 0; i < count; i++ {
		if len(buf) < 100 {
			buf = make([]byte, 8096)
			rand.Read(buf)
		}
		k := rand.Intn(51) + 50
		c := chunks.NewChunk(buf[:k])
		buf = buf[k:]
		compressed[addr(c.Hash())] = ChunkToCompressedChunk(c)
	}
	return
}
