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
	"context"
	"encoding/base32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestJournalWriterReadWrite(t *testing.T) {
	type opKind byte

	type operation struct {
		kind   opKind
		buf    []byte
		readAt int64
	}

	const (
		readOp opKind = iota
		writeOp
		flushOp
	)

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
			name: "successive writes trigger buffer flush ",
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
			path := newTestFilePath(t)
			j := newTestJournalWriter(t, path)
			// set specific buffer size
			j.buf = make([]byte, 0, test.size)

			var off int64
			var err error
			for i, op := range test.ops {
				switch op.kind {
				case readOp:
					act := make([]byte, len(op.buf))
					n, err := j.readAt(act, op.readAt)
					assert.NoError(t, err, "operation %d errored", i)
					assert.Equal(t, len(op.buf), n, "operation %d failed", i)
					assert.Equal(t, op.buf, act, "operation %d failed", i)
				case writeOp:
					var p []byte
					p, err = j.getBytes(context.Background(), len(op.buf))
					require.NoError(t, err, "operation %d errored", i)
					n := copy(p, op.buf)
					assert.Equal(t, len(op.buf), n, "operation %d failed", i)
					off += int64(n)
				case flushOp:
					err = j.flush(context.Background())
					assert.NoError(t, err, "operation %d errored", i)
				default:
					t.Fatal("unknown opKind")
				}
				assert.Equal(t, off, j.offset())
			}
		})
	}
}

func newTestJournalWriter(t *testing.T, path string) *journalWriter {
	ctx := context.Background()
	j, err := createJournalWriter(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, j)
	_, err = j.bootstrapJournal(ctx, nil)
	require.NoError(t, err)
	return j
}

func TestJournalWriterWriteCompressedChunk(t *testing.T) {
	path := newTestFilePath(t)
	j := newTestJournalWriter(t, path)
	data := randomCompressedChunks(1024)
	for a, cc := range data {
		err := j.writeCompressedChunk(context.Background(), cc)
		require.NoError(t, err)
		r, _ := j.ranges.get(a)
		validateLookup(t, j, r, cc)
	}
	validateAllLookups(t, j, data)
}

func TestJournalWriterBootstrap(t *testing.T) {
	ctx := context.Background()
	path := newTestFilePath(t)
	j := newTestJournalWriter(t, path)
	data := randomCompressedChunks(1024)
	var last hash.Hash
	for _, cc := range data {
		err := j.writeCompressedChunk(context.Background(), cc)
		require.NoError(t, err)
		last = cc.Hash()
	}
	require.NoError(t, j.commitRootHash(context.Background(), last))
	require.NoError(t, j.Close())

	j, _, err := openJournalWriter(ctx, path)
	require.NoError(t, err)
	reflogBuffer := newReflogRingBuffer(10)
	last, err = j.bootstrapJournal(ctx, reflogBuffer)
	require.NoError(t, err)
	assertExpectedIterationOrder(t, reflogBuffer, []string{last.String()})

	validateAllLookups(t, j, data)

	source := journalChunkSource{journal: j}
	for a, cc := range data {
		buf, _, err := source.get(ctx, a, nil, nil)
		require.NoError(t, err)
		ch, err := cc.ToChunk()
		require.NoError(t, err)
		assert.Equal(t, ch.Data(), buf)
	}
}

func validateAllLookups(t *testing.T, j *journalWriter, data map[hash.Hash]CompressedChunk) {
	// move |data| to addr16-keyed map
	prefixMap := make(map[addr16]CompressedChunk, len(data))
	var prefix addr16
	for a, cc := range data {
		copy(prefix[:], a[:])
		prefixMap[prefix] = cc
	}
	iterRangeIndex(j.ranges, func(a addr16, r Range) (stop bool) {
		validateLookup(t, j, r, prefixMap[a])
		return
	})
}

func iterRangeIndex(idx rangeIndex, cb func(addr16, Range) (stop bool)) {
	for h, r := range idx.novel {
		cb(toAddr16(h), r)
	}
	for a16, r := range idx.cached {
		cb(a16, r)
	}
}

func validateLookup(t *testing.T, j *journalWriter, r Range, cc CompressedChunk) {
	buf := make([]byte, r.Length)
	_, err := j.readAt(buf, int64(r.Offset))
	require.NoError(t, err)
	act, err := NewCompressedChunk(cc.H, buf)
	assert.NoError(t, err)
	assert.Equal(t, cc.FullCompressedChunk, act.FullCompressedChunk)
}

func TestJournalWriterSyncClose(t *testing.T) {
	path := newTestFilePath(t)
	j := newTestJournalWriter(t, path)
	p := []byte("sit")
	buf, err := j.getBytes(context.Background(), len(p))
	require.NoError(t, err)
	copy(buf, p)
	j.flush(context.Background())
	assert.Equal(t, 0, len(j.buf))
	assert.Equal(t, 3, int(j.off))
}

func newTestFilePath(t *testing.T) string {
	path, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	return filepath.Join(path, "journal.log")
}

func TestJournalIndexBootstrap(t *testing.T) {
	// potentially indexed region of a journal
	type epoch struct {
		records map[hash.Hash]CompressedChunk
		last    hash.Hash
	}

	makeEpoch := func() (e epoch) {
		e.records = randomCompressedChunks(8)
		for h := range e.records {
			e.last = hash.Hash(h)
			break
		}
		return
	}

	tests := []struct {
		name   string
		epochs []epoch
		novel  epoch
	}{
		{
			name:   "smoke test",
			epochs: []epoch{makeEpoch()},
		},
		{
			name:   "non-indexed journal",
			epochs: nil,
			novel:  makeEpoch(),
		},
		{
			name:   "partially indexed journal",
			epochs: []epoch{makeEpoch()},
			novel:  makeEpoch(),
		},
		{
			name: "multiple index records",
			epochs: []epoch{
				makeEpoch(),
				makeEpoch(),
				makeEpoch(),
			},
			novel: makeEpoch(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := newTestFilePath(t)
			j := newTestJournalWriter(t, path)
			// setup
			var recordCnt int
			epochs := append(test.epochs, test.novel)
			for i, e := range epochs {
				for _, cc := range e.records {
					recordCnt++
					assert.NoError(t, j.writeCompressedChunk(context.Background(), cc))
					if rand.Int()%10 == 0 { // periodic commits
						assert.NoError(t, j.commitRootHash(context.Background(), cc.H))
					}
				}
				o := j.offset()                                                   // precommit offset
				assert.NoError(t, j.commitRootHash(context.Background(), e.last)) // commit |e.last|
				if i == len(epochs) {
					break // don't index |test.novel|
				}
				assert.NoError(t, j.flushIndexRecord(ctx, e.last, o)) // write index record
			}
			err := j.Close()
			require.NoError(t, err)

			validateJournal := func(p string, expected []epoch) {
				journal, ok, err := openJournalWriter(ctx, p)
				require.NoError(t, err)
				require.True(t, ok)
				// bootstrap journal and validate chunk records
				last, err := journal.bootstrapJournal(ctx, nil)
				assert.NoError(t, err)
				for _, e := range expected {
					var act CompressedChunk
					for a, exp := range e.records {
						act, err = journal.getCompressedChunk(a)
						assert.NoError(t, err)
						assert.Equal(t, exp, act)
					}
				}
				assert.Equal(t, expected[len(expected)-1].last, last)
				assert.NoError(t, journal.Close())
			}

			idxPath := filepath.Join(filepath.Dir(path), journalIndexFileName)

			before, err := os.Stat(idxPath)
			require.NoError(t, err)

			lookupSize := int64(recordCnt * (1 + lookupSz))
			metaSize := int64(len(epochs)) * (1 + lookupMetaSz)
			assert.Equal(t, lookupSize+metaSize, before.Size())

			// bootstrap journal using index
			validateJournal(path, epochs)
			// assert journal index unchanged
			info, err := os.Stat(idxPath)
			require.NoError(t, err)
			assert.Equal(t, before.Size(), info.Size())

			// bootstrap journal with corrupted index
			corruptJournalIndex(t, idxPath)
			jnl, ok, err := openJournalWriter(ctx, idxPath)
			require.NoError(t, err)
			require.True(t, ok)
			_, err = jnl.bootstrapJournal(ctx, nil)
			assert.Error(t, err)
		})
	}
}

var encoding = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv")

// encode returns the base32 encoding in the Dolt alphabet.
func encode(data []byte) string {
	return encoding.EncodeToString(data)
}

func randomCompressedChunks(cnt int) (compressed map[hash.Hash]CompressedChunk) {
	compressed = make(map[hash.Hash]CompressedChunk)
	var buf []byte
	for i := 0; i < cnt; i++ {
		k := rand.Intn(51) + 50
		if k >= len(buf) {
			buf = make([]byte, 64*1024)
			rand.Read(buf)
		}
		c := chunks.NewChunk(buf[:k])
		buf = buf[k:]
		compressed[c.Hash()] = ChunkToCompressedChunk(c)
	}
	return
}

func corruptJournalIndex(t *testing.T, path string) {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)
	buf := make([]byte, 64)
	rand.Read(buf)
	_, err = f.WriteAt(buf, info.Size()/2)
	require.NoError(t, err)
}

func TestRangeIndex(t *testing.T) {
	data := randomCompressedChunks(1024)
	idx := newRangeIndex()
	for _, c := range data {
		idx.put(c.Hash(), Range{})
	}
	for _, c := range data {
		_, ok := idx.get(c.Hash())
		assert.True(t, ok)
	}
	assert.Equal(t, len(data), idx.novelCount())
	assert.Equal(t, len(data), int(idx.count()))
	idx = idx.flatten(context.Background())
	assert.Equal(t, 0, idx.novelCount())
	assert.Equal(t, len(data), int(idx.count()))
}
