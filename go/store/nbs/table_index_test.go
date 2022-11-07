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
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.chunkCount(); i++ {
		var onheapaddr addr
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
