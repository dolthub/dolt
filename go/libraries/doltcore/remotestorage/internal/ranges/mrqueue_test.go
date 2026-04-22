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

package ranges

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMRQueue_Empty(t *testing.T) {
	q := NewMRQueue()
	assert.Equal(t, 0, q.Len())
	groups, url, dark := q.PopRequest(1<<20, 128, 256)
	assert.Nil(t, groups)
	assert.Equal(t, "", url)
	assert.Equal(t, uint64(0), dark)
}

func TestMRQueue_SingleChunk(t *testing.T) {
	q := NewMRQueue()
	q.Insert("u1", nil, 100, 50, 0, 0)
	assert.Equal(t, 1, q.Len())

	groups, url, dark := q.PopRequest(1<<20, 128, 256)
	assert.Equal(t, "u1", url)
	assert.Equal(t, uint64(0), dark)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, 1, len(groups[0]))
	assert.Equal(t, uint64(100), groups[0][0].Offset)
	assert.Equal(t, 0, q.Len())
}

func TestMRQueue_AdjacentBridgeWithinSlop(t *testing.T) {
	q := NewMRQueue()
	// Two chunks with a 200-byte gap; slop=256 means they should
	// bridge into a single group.
	q.Insert("u1", nil, 0, 100, 0, 0)
	q.Insert("u1", nil, 300, 100, 0, 0)

	groups, _, dark := q.PopRequest(1<<20, 128, 256)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, 2, len(groups[0]))
	assert.Equal(t, uint64(200), dark)
}

func TestMRQueue_GapExceedsSlopSplitsGroups(t *testing.T) {
	q := NewMRQueue()
	// Gap of 500 > slop of 256: two groups.
	q.Insert("u1", nil, 0, 100, 0, 0)
	q.Insert("u1", nil, 600, 100, 0, 0)

	groups, _, dark := q.PopRequest(1<<20, 128, 256)
	assert.Equal(t, 2, len(groups))
	assert.Equal(t, 1, len(groups[0]))
	assert.Equal(t, 1, len(groups[1]))
	assert.Equal(t, uint64(0), dark)
}

func TestMRQueue_MaxRangesCap(t *testing.T) {
	q := NewMRQueue()
	// Three chunks, each gap > slop. With maxRanges=2, only the
	// first two should be popped.
	q.Insert("u1", nil, 0, 100, 0, 0)
	q.Insert("u1", nil, 1000, 100, 0, 0)
	q.Insert("u1", nil, 2000, 100, 0, 0)

	groups, _, _ := q.PopRequest(1<<20, 2, 256)
	assert.Equal(t, 2, len(groups))
	assert.Equal(t, 1, q.Len())

	// Next pop drains the last chunk.
	groups, _, _ = q.PopRequest(1<<20, 2, 256)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, 0, q.Len())
}

func TestMRQueue_MaxBytesCap(t *testing.T) {
	q := NewMRQueue()
	// Three adjacent chunks of 100 each, gap 0. maxBytes=250 lets
	// us take 2 (total 200) but not the third (would be 300).
	q.Insert("u1", nil, 0, 100, 0, 0)
	q.Insert("u1", nil, 100, 100, 0, 0)
	q.Insert("u1", nil, 200, 100, 0, 0)

	groups, _, _ := q.PopRequest(250, 128, 256)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, 2, len(groups[0]))
	assert.Equal(t, 1, q.Len())
}

func TestMRQueue_FirstChunkOverBudget(t *testing.T) {
	q := NewMRQueue()
	// Single chunk larger than maxBytes — must still be taken
	// so the caller makes forward progress.
	q.Insert("u1", nil, 0, 1000, 0, 0)
	groups, _, _ := q.PopRequest(100, 128, 256)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, 0, q.Len())
}

func TestMRQueue_RoundRobinAcrossURLs(t *testing.T) {
	q := NewMRQueue()
	q.Insert("a", nil, 0, 10, 0, 0)
	q.Insert("b", nil, 0, 10, 0, 0)
	q.Insert("c", nil, 0, 10, 0, 0)

	_, u1, _ := q.PopRequest(1<<20, 128, 256)
	_, u2, _ := q.PopRequest(1<<20, 128, 256)
	_, u3, _ := q.PopRequest(1<<20, 128, 256)
	assert.Equal(t, "a", u1)
	assert.Equal(t, "b", u2)
	assert.Equal(t, "c", u3)
	assert.Equal(t, 0, q.Len())
}

func TestMRQueue_DrainsAfterEmptyURLs(t *testing.T) {
	q := NewMRQueue()
	q.Insert("a", nil, 0, 10, 0, 0)
	q.Insert("b", nil, 0, 10, 0, 0)
	// Drain a first.
	_, u1, _ := q.PopRequest(1<<20, 128, 256)
	assert.Equal(t, "a", u1)
	// Next pop must return b even though the cursor might point past a.
	_, u2, _ := q.PopRequest(1<<20, 128, 256)
	assert.Equal(t, "b", u2)
}
