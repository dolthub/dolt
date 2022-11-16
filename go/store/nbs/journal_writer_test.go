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
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

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
		init []byte
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
			init: []byte("loremipsum"),
			ops: []operation{
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
			name: "write larger that buffer",
			size: 8,
			init: []byte("loremipsum"),
			ops: []operation{
				{kind: flushOp},
				{kind: writeOp, buf: []byte("dolorsitamet")},
				{kind: readOp, buf: []byte("dolorsitamet"), readAt: 10},
				{kind: readOp, buf: []byte("loremipsumdolorsitamet"), readAt: 0},
			},
		},
		{
			name: "flush empty buffer",
			size: 16,
			init: []byte("loremipsum"),
			ops: []operation{
				{kind: flushOp},
			},
		},
		{
			name: "double flush write",
			size: 16,
			init: []byte("loremipsum"),
			ops: []operation{
				{kind: writeOp, buf: []byte("dolor")},
				{kind: flushOp},
				{kind: flushOp},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name+"journal.Write()", func(t *testing.T) {
			f := newTestFile(t)
			n, err := f.Write(test.init)
			require.NoError(t, err)
			j := newJournalWriter(f, int64(n), test.size)

			var off = int64(n)
			for i, op := range test.ops {
				switch op.kind {
				case readOp:
					act := make([]byte, len(op.buf))
					n, err = j.ReadAt(act, op.readAt)
					assert.NoError(t, err, "operation %d errored", i)
					assert.Equal(t, len(op.buf), n, "operation %d failed", i)
					assert.Equal(t, op.buf, act, "operation %d failed", i)
				case writeOp:
					n, err = j.Write(op.buf)
					assert.NoError(t, err, "operation %d errored", i)
					assert.Equal(t, len(op.buf), n, "operation %d failed", i)
					off += int64(n)
				case flushOp:
					err = j.Flush()
					assert.NoError(t, err, "operation %d errored", i)
				default:
					t.Fatal("unknown opKind")
				}
				assert.Equal(t, off, j.Offset())
			}
			assert.NoError(t, j.Close())
		})
	}
}

func TestJournalWriterSyncClose(t *testing.T) {
	f := newTestFile(t)
	n, err := f.Write([]byte("loremipsum"))
	require.NoError(t, err)
	j := newJournalWriter(f, int64(n), 16)

	// sync triggers flush
	n, err = j.Write([]byte("dolor"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	err = j.Sync()
	require.NoError(t, err)
	assert.Equal(t, 0, len(j.buf))
	assert.Equal(t, 15, int(j.off))

	// close triggers flush
	n, err = j.Write([]byte("sit"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	err = j.Close()
	require.NoError(t, err)
	assert.Equal(t, 0, len(j.buf))
	assert.Equal(t, 18, int(j.off))
}

func newTestFile(t *testing.T) *os.File {
	name := fmt.Sprintf("journal%d.log", rand.Intn(65536))
	f, err := os.Create(path.Join(t.TempDir(), name))
	require.NoError(t, err)
	return f
}
