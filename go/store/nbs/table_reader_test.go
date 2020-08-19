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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/assert"
)

func TestCompressedChunkIsEmpty(t *testing.T) {
	if !EmptyCompressedChunk.IsEmpty() {
		t.Fatal("EmptyCompressedChunkIsEmpty() should equal true.")
	}
	if !(CompressedChunk{}).IsEmpty() {
		t.Fatal("CompressedChunk{}.IsEmpty() should equal true.")
	}
}

func TestParseTableIndex(t *testing.T) {
	f, err := os.Open("testdata/0oa7mch34jg1rvghrnhr4shrp2fm4ftd.idx")
	require.NoError(t, err)
	defer f.Close()
	bs, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndex(bs)
	require.NoError(t, err)
	defer idx.Close()
	assert.Equal(t, uint32(596), idx.ChunkCount())
	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.ChunkCount(); i++ {
		var onheapaddr addr
		e := idx.IndexEntry(i, &onheapaddr)
		if _, ok := seen[onheapaddr]; !ok {
			seen[onheapaddr] = true
			lookupe, ok := idx.Lookup(&onheapaddr)
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
	bs, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	idx, err := parseTableIndex(bs)
	require.NoError(t, err)
	defer idx.Close()
	mmidx, err := newMmapTableIndex(idx, nil)
	require.NoError(t, err)
	defer mmidx.Close()
	assert.Equal(t, idx.ChunkCount(), mmidx.ChunkCount())
	seen := make(map[addr]bool)
	for i := uint32(0); i < idx.ChunkCount(); i++ {
		var onheapaddr addr
		onheapentry := idx.IndexEntry(i, &onheapaddr)
		var mmaddr addr
		mmentry := mmidx.IndexEntry(i, &mmaddr)
		assert.Equal(t, onheapaddr, mmaddr)
		assert.Equal(t, onheapentry.Offset(), mmentry.Offset())
		assert.Equal(t, onheapentry.Length(), mmentry.Length())
		if _, ok := seen[onheapaddr]; !ok {
			seen[onheapaddr] = true
			mmentry, found := mmidx.Lookup(&onheapaddr)
			assert.True(t, found)
			assert.Equal(t, onheapentry.Offset(), mmentry.Offset(), "%v does not match %v for address %v", onheapentry, mmentry, onheapaddr)
			assert.Equal(t, onheapentry.Length(), mmentry.Length())
		}
		wrongaddr := onheapaddr
		if wrongaddr[19] != 0 {
			wrongaddr[19] = 0
			_, found := mmidx.Lookup(&wrongaddr)
			assert.False(t, found)
		}
	}

	assert.Equal(t, idx.Ordinals(), mmidx.Ordinals())
	assert.Equal(t, idx.Prefixes(), mmidx.Prefixes())
	assert.Equal(t, idx.TableFileSize(), mmidx.TableFileSize())
	assert.Equal(t, idx.TotalUncompressedData(), mmidx.TotalUncompressedData())
}
