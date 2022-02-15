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
	idx, err := parseTableIndexByCopy(bs)
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
	idx, err := parseTableIndexByCopy(bs)
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
