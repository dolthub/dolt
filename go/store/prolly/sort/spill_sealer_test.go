// Copyright 2026 Dolthub, Inc.
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

package sort

import (
	"bytes"
	"io"
	"os"
	"sync/atomic"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// toySpillSealer stores a spill file's records as its tuples XOR 0x5a, after a marker.
type toySpillSealer struct {
	files, sealed, opened atomic.Int64
}

var toyMarker = []byte("toy!")

type toySpillFile struct {
	s   *toySpillSealer
	buf []byte
}

func (s *toySpillSealer) NewFile(int) SpillFileSealer {
	s.files.Add(1)
	return &toySpillFile{s: s}
}

func xor(dst, b []byte) []byte {
	for _, c := range b {
		dst = append(dst, c^0x5a)
	}
	return dst
}

func (f *toySpillFile) Seal(k val.Tuple) val.Tuple {
	f.s.sealed.Add(1)
	f.buf = xor(append(f.buf[:0], toyMarker...), k)
	return f.buf
}

func (f *toySpillFile) Reader() SpillFileReader {
	return toySpillReader{f.s}
}

type toySpillReader struct {
	s *toySpillSealer
}

func (r toySpillReader) Next(rd io.Reader) (val.Tuple, error) {
	var size [keyLenSz]byte
	if _, err := io.ReadFull(rd, size[:]); err != nil {
		return nil, err
	}
	rec := make([]byte, readUint32(size[:]))
	if _, err := io.ReadFull(rd, rec); err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(rec, toyMarker) {
		return nil, io.ErrUnexpectedEOF
	}
	r.s.opened.Add(1)
	return xor(nil, rec[len(toyMarker):]), nil
}

func TestSpillSealer(t *testing.T) {
	ctx := sql.NewEmptyContext()
	td := val.NewTupleDescriptor(val.Type{Enc: val.Int64Enc, Nullable: false}, val.Type{Enc: val.StringEnc, Nullable: false})
	keyCmp := func(l, r val.Tuple) bool {
		cmp, err := td.Compare(ctx, l, r)
		require.NoError(t, err)
		return cmp <= 0
	}
	ns := tree.NewTestNodeStore()
	keys := testTuples(ns, td, 2000)
	tmpProv := newProv(t)
	defer tmpProv.Clean()

	s := NewTupleSorter(2048, 4, keyCmp, tmpProv)
	defer s.Close()
	sealer := &toySpillSealer{}
	s.SetSpillSealer(sealer)
	for _, k := range keys {
		require.NoError(t, s.Insert(ctx, k))
	}
	iterable, err := s.Flush(ctx)
	require.NoError(t, err)
	kf, ok := iterable.(*keyFile)
	require.True(t, ok, "the sorter spilled to disk")
	stored, err := os.ReadFile(kf.f.Name())
	require.NoError(t, err)
	assert.False(t, bytes.Contains(stored, keys[0]), "spilled tuples are sealed")

	iter, err := iterable.IterAll(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var got []val.Tuple
	for {
		k, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, k)
	}
	require.Len(t, got, len(keys))
	ensureSorted(t, got, keyCmp)
	assert.Greater(t, sealer.files.Load(), int64(1))
	assert.Equal(t, sealer.sealed.Load(), sealer.opened.Load(), "every sealed record is opened once")
}
