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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestParseTableIndex(t *testing.T) {
	ctx := context.Background()
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndexByCopy(ctx, bs, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	defer idx.Close()
	assert.Equal(t, uint32(596), idx.chunkCount())
	seen := make(map[hash.Hash]bool)
	for i := uint32(0); i < idx.chunkCount(); i++ {
		var onheapaddr hash.Hash
		e, err := idx.indexEntry(i, &onheapaddr)
		require.NoError(t, err)
		if _, ok := seen[onheapaddr]; !ok {
			seen[onheapaddr] = true
			lookupe, ok, err := idx.lookup(&onheapaddr)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, e.Offset(), lookupe.Offset(), "%v does not match %v for address %v", e, lookupe, onheapaddr)
			assert.Equal(t, e.Length(), lookupe.Length())
		}
	}
}

func TestParseLargeTableIndex(t *testing.T) {
	if isRaceEnabled() {
		t.SkipNow()
	}

	// This is large enough for the NBS table index to overflow uint32s on certain index calculations.
	numChunks := uint32(320331063)
	idxSize := indexSize(numChunks)
	sz := idxSize + footerSize
	idxBuf := make([]byte, sz)
	copy(idxBuf[idxSize+12:], magicNumber)
	binary.BigEndian.PutUint32(idxBuf[idxSize:], numChunks)
	binary.BigEndian.PutUint64(idxBuf[idxSize+4:], uint64(numChunks)*4*1024)

	var prefix uint64

	off := 0
	// Write Tuples
	for i := uint32(0); i < numChunks; i++ {
		binary.BigEndian.PutUint64(idxBuf[off:], prefix)
		binary.BigEndian.PutUint32(idxBuf[off+hash.PrefixLen:], i)
		prefix += 2
		off += prefixTupleSize
	}

	// Write Lengths
	for i := uint32(0); i < numChunks; i++ {
		binary.BigEndian.PutUint32(idxBuf[off:], 4*1024)
		off += lengthSize
	}

	// Write Suffixes
	for i := uint32(0); i < numChunks; i++ {
		off += hash.SuffixLen
	}

	idx, err := parseTableIndex(context.Background(), idxBuf, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	h := &hash.Hash{}
	h[7] = 2
	ord, err := idx.lookupOrdinal(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), ord)
	h[7] = 1
	ord, err = idx.lookupOrdinal(h)
	require.NoError(t, err)
	assert.Equal(t, numChunks, ord)
	// This is the end of the chunk, not the beginning.
	assert.Equal(t, uint64(8*1024), idx.offsetAt(1))
	assert.Equal(t, uint64(2), idx.prefixAt(1))
	assert.Equal(t, uint32(1), idx.ordinalAt(1))
	h[7] = 2
	assert.Equal(t, *h, idx.hashAt(1))
	entry, ok, err := idx.lookup(h)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(4*1024), entry.Offset())
	assert.Equal(t, uint32(4*1024), entry.Length())
}

func BenchmarkFindPrefix(b *testing.B) {
	ctx := context.Background()
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(b, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(b, err)
	idx, err := parseTableIndexByCopy(ctx, bs, &UnlimitedQuotaProvider{})
	require.NoError(b, err)
	defer idx.Close()
	assert.Equal(b, uint32(596), idx.chunkCount())

	prefixes, err := idx.prefixes()
	require.NoError(b, err)

	b.Run("benchmark prefixIdx()", func(b *testing.B) {
		var ord uint32
		for i := 0; i < b.N; i++ {
			ord = prefixIdx(idx, prefixes[uint(i)&uint(512)])
		}
		assert.True(b, ord < 596)
	})
	b.Run("benchmark findPrefix", func(b *testing.B) {
		var ord uint32
		for i := 0; i < b.N; i++ {
			ord = idx.findPrefix(prefixes[uint(i)&uint(512)])
		}
		assert.True(b, ord < 596)
	})
}

func TestFindPrefix(t *testing.T) {
	// Test some edge cases in findPrefix.
	var idx onHeapTableIndex
	idx.count = 1
	idx.prefixTuples = make([]byte, 12)
	assert.Equal(t, uint32(0), idx.findPrefix(0))
	assert.Equal(t, uint32(1), idx.findPrefix(1))
	binary.BigEndian.PutUint64(idx.prefixTuples[:], 1)
	assert.Equal(t, uint32(0), idx.findPrefix(0))
	assert.Equal(t, uint32(0), idx.findPrefix(1))
	assert.Equal(t, uint32(1), idx.findPrefix(2))

	// Enough so that the index * 12 (prefix tuple size) will overflow a uint32
	idx.prefixTuples = make([]byte, 1<<30*12)
	idx.count = 1 << 30
	for i := 0; i < len(idx.prefixTuples); i += 12 {
		binary.BigEndian.PutUint64(idx.prefixTuples[i:], uint64(i))
	}
	assert.Equal(t, uint32(0), idx.findPrefix(0))
	assert.Equal(t, uint32(1), idx.findPrefix(1))
	assert.Equal(t, uint32(1), idx.findPrefix(12))
	assert.Equal(t, uint32(2), idx.findPrefix(13))
	assert.Equal(t, uint32(idx.count)-1, idx.findPrefix(((1<<30)*12)-12))
	assert.Equal(t, uint32(idx.count), idx.findPrefix(((1<<30)*12)-11))
	assert.Equal(t, uint32(idx.count), idx.findPrefix(((1<<30)*12)+12))

	assert.Equal(t, uint32(0x18d5555), idx.findPrefix(312475644))
}

// previous implementation for findIndex().
func prefixIdx(ti onHeapTableIndex, prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, ti.chunkCount()
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		if ti.prefixAt(h) < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	return
}

func TestOnHeapTableIndex_ResolveShortHash(t *testing.T) {
	ctx := context.Background()
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndexByCopy(ctx, bs, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	defer idx.Close()
	res, err := idx.ResolveShortHash([]byte("0"))
	require.NoError(t, err)
	t.Log("matched: ", len(res))
	for _, h := range res {
		t.Log("\t", h)
	}
}

func TestResolveOneHash(t *testing.T) {
	ctx := context.Background()
	// create chunks
	chunks := [][]byte{
		[]byte("chunk1"),
	}

	// build table index
	td, _, err := buildTable(chunks)
	tIdx, err := parseTableIndexByCopy(ctx, td, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	defer tIdx.Close()

	// get hashes out
	hashes := make([]string, len(chunks))
	for i, c := range chunks {
		hashes[i] = computeAddr(c).String()
		t.Log(hashes[i])
	}

	// resolve them
	for _, h := range hashes {
		// try every length
		for i := 0; i < 32; i++ {
			res, err := tIdx.ResolveShortHash([]byte(h[:i]))
			require.NoError(t, err)
			assert.Equal(t, 1, len(res))
		}
	}
}

func TestResolveFewHash(t *testing.T) {
	ctx := context.Background()
	// create chunks
	chunks := [][]byte{
		[]byte("chunk1"),
		[]byte("chunk2"),
		[]byte("chunk3"),
	}

	// build table index
	td, _, err := buildTable(chunks)
	tIdx, err := parseTableIndexByCopy(ctx, td, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	defer tIdx.Close()

	// get hashes out
	hashes := make([]string, len(chunks))
	for i, c := range chunks {
		hashes[i] = computeAddr(c).String()
		t.Log(hashes[i])
	}

	// resolve them
	for _, h := range hashes {
		// try every length
		for i := 0; i < 32; i++ {
			res, err := tIdx.ResolveShortHash([]byte(h[:i]))
			require.NoError(t, err)
			t.Log("asserting length: ", i)
			assert.Less(t, 0, len(res))
		}
	}
}

func TestAmbiguousShortHash(t *testing.T) {
	ctx := context.Background()
	// create chunks
	chunks := []fakeChunk{
		{address: addrFromPrefix("abcdef"), data: fakeData},
		{address: addrFromPrefix("abctuv"), data: fakeData},
		{address: addrFromPrefix("abcd123"), data: fakeData},
	}

	// build table index
	td, _, err := buildFakeChunkTable(chunks)
	idx, err := parseTableIndexByCopy(ctx, td, &UnlimitedQuotaProvider{})
	require.NoError(t, err)
	defer idx.Close()

	tests := []struct {
		pre string
		sz  int
	}{
		{pre: "", sz: 3},
		{pre: "a", sz: 3},
		{pre: "b", sz: 0},
		{pre: "v", sz: 0},
		{pre: "ab", sz: 3},
		{pre: "abc", sz: 3},
		{pre: "abcd", sz: 2},
		{pre: "abct", sz: 1},
		{pre: "abcde", sz: 1},
		{pre: "abcd1", sz: 1},
		{pre: "abcdef", sz: 1},
		{pre: "abctuv", sz: 1},
		{pre: "abcd123", sz: 1},
	}

	for _, test := range tests {
		name := fmt.Sprintf("Expect %d results for prefix %s", test.sz, test.pre)
		t.Run(name, func(t *testing.T) {
			res, err := idx.ResolveShortHash([]byte(test.pre))
			require.NoError(t, err)
			assert.Len(t, res, test.sz)
		})
	}
}

func TestReadTableFooter(t *testing.T) {
	// Less than 20 bytes is not enough to read the footer
	reader := bytes.NewReader(make([]byte, 19))
	_, _, err := ReadTableFooter(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "negative position")

	data := make([]byte, 20)
	binary.BigEndian.PutUint32(data[:4], 98765)   // Chunk Count.
	binary.BigEndian.PutUint64(data[4:12], 12345) // Total Size
	copy(data[12:], magicNumber)
	reader = bytes.NewReader(data)
	chunkCount, totalSize, err := ReadTableFooter(reader)
	assert.NoError(t, err)
	assert.Equal(t, uint32(98765), chunkCount)
	assert.Equal(t, uint64(12345), totalSize)

	// Now with a future magic number
	data[12] = 0
	copy(data[13:], doltMagicNumber)
	reader = bytes.NewReader(data)
	_, _, err = ReadTableFooter(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported table file format")

	// Now with corrupted info that we don't recognize.
	copy(data[12:], "DEADBEEF")
	reader = bytes.NewReader(data)
	_, _, err = ReadTableFooter(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid or corrupt table file")
}

// fakeChunk is chunk with a faked address
type fakeChunk struct {
	address hash.Hash
	data    []byte
}

var fakeData = []byte("supercalifragilisticexpialidocious")

func addrFromPrefix(prefix string) hash.Hash {
	// create a full length addr from a prefix
	for {
		if len(prefix) < hash.StringLen {
			prefix += "0"
		} else {
			break
		}
	}
	return hash.Parse(prefix)
}

func buildFakeChunkTable(chunks []fakeChunk) ([]byte, hash.Hash, error) {
	totalData := uint64(0)
	for _, chunk := range chunks {
		totalData += uint64(len(chunk.data))
	}
	capacity := maxTableSize(uint64(len(chunks)), totalData)

	buff := make([]byte, capacity)

	tw := newTableWriter(buff, nil)

	for _, chunk := range chunks {
		tw.addChunk(chunk.address, chunk.data)
	}

	length, blockHash, err := tw.finish()

	if err != nil {
		return nil, hash.Hash{}, err
	}

	return buff[:length], blockHash, nil
}
