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
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTableIndex(t *testing.T) {
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndexByCopy(bs, &noopQuotaProvider{})
	require.NoError(t, err)
	defer idx.Close()
	assert.Equal(t, uint32(596), idx.ChunkCount())
	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.ChunkCount(); i++ {
		var onheapaddr addr
		e, err := idx.IndexEntry(i, &onheapaddr)
		require.NoError(t, err)
		if _, ok := seen[onheapaddr]; !ok {
			seen[onheapaddr] = true
			lookupe, ok, err := idx.Lookup(&onheapaddr)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, e.Offset(), lookupe.Offset(), "%v does not match %v for address %v", e, lookupe, onheapaddr)
			assert.Equal(t, e.Length(), lookupe.Length())
		}
	}
}

func TestMMapIndex(t *testing.T) {
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndexByCopy(bs, &noopQuotaProvider{})
	require.NoError(t, err)
	defer idx.Close()
	mmidx, err := newMmapTableIndex(idx, nil)
	require.NoError(t, err)
	defer mmidx.Close()
	assert.Equal(t, idx.ChunkCount(), mmidx.ChunkCount())
	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.ChunkCount(); i++ {
		var onheapaddr addr
		onheapentry, err := idx.IndexEntry(i, &onheapaddr)
		require.NoError(t, err)
		var mmaddr addr
		mmentry, err := mmidx.IndexEntry(i, &mmaddr)
		require.NoError(t, err)
		assert.Equal(t, onheapaddr, mmaddr)
		assert.Equal(t, onheapentry.Offset(), mmentry.Offset())
		assert.Equal(t, onheapentry.Length(), mmentry.Length())
		if _, ok := seen[onheapaddr]; !ok {
			seen[onheapaddr] = true
			mmentry, found, err := mmidx.Lookup(&onheapaddr)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, onheapentry.Offset(), mmentry.Offset(), "%v does not match %v for address %v", onheapentry, mmentry, onheapaddr)
			assert.Equal(t, onheapentry.Length(), mmentry.Length())
		}
		wrongaddr := onheapaddr
		if wrongaddr[19] != 0 {
			wrongaddr[19] = 0
			_, found, err := mmidx.Lookup(&wrongaddr)
			require.NoError(t, err)
			assert.False(t, found)
		}
	}
	o1, err := idx.Ordinals()
	require.NoError(t, err)
	o2, err := mmidx.Ordinals()
	require.NoError(t, err)
	assert.Equal(t, o1, o2)
	p1, err := idx.Prefixes()
	require.NoError(t, err)
	p2, err := mmidx.Prefixes()
	require.NoError(t, err)
	assert.Equal(t, p1, p2)
	assert.Equal(t, idx.TableFileSize(), mmidx.TableFileSize())
	assert.Equal(t, idx.TotalUncompressedData(), mmidx.TotalUncompressedData())
}

func TestOnHeapTableIndex_ResolveShortHash(t *testing.T) {
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := io.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndexByCopy(bs, &noopQuotaProvider{})
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
	// create chunks
	chunks := [][]byte{
		[]byte("chunk1"),
	}

	// build table index
	td, _, err := buildTable(chunks)
	tIdx, err := parseTableIndexByCopy(td, &noopQuotaProvider{})
	require.NoError(t, err)

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
	// create chunks
	chunks := [][]byte{
		[]byte("chunk1"),
		[]byte("chunk2"),
		[]byte("chunk3"),
	}

	// build table index
	td, _, err := buildTable(chunks)
	tIdx, err := parseTableIndexByCopy(td, &noopQuotaProvider{})
	require.NoError(t, err)

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
	// create chunks
	chunks := []fakeChunk{
		{address: addrFromPrefix("abcdef"), data: fakeData},
		{address: addrFromPrefix("abctuv"), data: fakeData},
		{address: addrFromPrefix("abcd123"), data: fakeData},
	}

	// build table index
	td, _, err := buildFakeChunkTable(chunks)
	idx, err := parseTableIndexByCopy(td, &noopQuotaProvider{})
	require.NoError(t, err)

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

// fakeChunk is chunk with a faked address
type fakeChunk struct {
	address addr
	data    []byte
}

var fakeData = []byte("supercalifragilisticexpialidocious")

func addrFromPrefix(prefix string) (a addr) {
	// create a full length addr from a prefix
	for i := 0; i < addrSize; i++ {
		prefix += "0"
	}

	// base32 decode string
	h, _ := encoding.DecodeString(prefix)
	copy(a[:], h)
	return
}

func buildFakeChunkTable(chunks []fakeChunk) ([]byte, addr, error) {
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
		return nil, addr{}, err
	}

	return buff[:length], blockHash, nil
}
