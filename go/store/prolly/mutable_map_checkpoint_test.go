// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

func TestMutableMapCheckpoints(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := "test mutable map at scale " + strconv.Itoa(s)
		t.Run(name, func(t *testing.T) {
			t.Run("stash", func(t *testing.T) {
				testCheckpoint(t, s)
			})
			t.Run("revert pre-flush", func(t *testing.T) {
				testRevertBeforeFlush(t, s)
			})
			t.Run("revert post-flush", func(t *testing.T) {
				testRevertAfterFlush(t, s)
			})
			t.Run("revert post-flush without checkpointed edits", func(t *testing.T) {
				testRevertAfterFlushWithoutCheckpointedEdits(t, s)
			})
			t.Run("revert twice post-flush", func(t *testing.T) {
				testRevertTwiceAfterFlush(t, s)
			})
			t.Run("revert post-two-flushes", func(t *testing.T) {
				testRevertAfterTwoFlushes(t, s)
			})
			t.Run("visit gc roots flushes checkpoint", func(t *testing.T) {
				testVisitGCRootsFlushesCheckpoint(t, s)
			})
		})
	}
}

func testCheckpoint(t *testing.T, scale int) {
	// create map with |s| even int64s
	ctx := context.Background()
	m := ascendingIntMapWithStep(t, scale, 2)
	mut := checkInvariants(t, m.Mutate())

	edits := ascendingTuplesWithStepAndStart(scale/10, 2, 1)

	for i, ed := range edits {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.False(t, ok)

		err = mut.Put(ctx, ed[0], ed[1])
		require.NoError(t, err)
		ok, err = mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.True(t, ok)

		err = mut.Checkpoint(ctx)
		assert.NoError(t, err)

		for j := 0; j < i; j++ {
			ok, err = mut.Has(ctx, edits[j][0])
			require.NoError(t, err)
			assert.True(t, ok)
		}
	}
}

func testRevertBeforeFlush(t *testing.T, scale int) {
	// create map with |s| even int64s
	ctx := context.Background()
	m := ascendingIntMapWithStep(t, scale, 2)
	mut := checkInvariants(t, m.Mutate())

	// create 2 edit sets: pre- and post- checkpoint
	edits := ascendingTuplesWithStepAndStart(scale/5, 2, 1)
	pre, post := edits[:scale/10], edits[scale/10:]

	for _, ed := range pre {
		err := mut.Put(ctx, ed[0], ed[1])
		require.NoError(t, err)
	}

	err := mut.Checkpoint(ctx)
	require.NoError(t, err)

	for _, ed := range post {
		err = mut.Put(ctx, ed[0], ed[1])
		require.NoError(t, err)
	}

	for _, ed := range edits {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.True(t, ok)
	}

	mut.Revert(ctx)

	for _, ed := range pre {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.True(t, ok)
	}
	for _, ed := range post {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.False(t, ok)
	}
}

func testRevertAfterFlush(t *testing.T, scale int) {
	// create map with |s| even int64s
	ctx := context.Background()
	m := ascendingIntMapWithStep(t, scale, 2)
	mut := checkInvariants(t, m.Mutate())

	// create 2 edit sets: pre- and post- checkpoint
	edits := ascendingTuplesWithStepAndStart(scale/5, 2, 1)
	pre, post := edits[:scale/10], edits[scale/10:]

	for _, ed := range pre {
		err := mut.Put(ctx, ed[0], ed[1])
		require.NoError(t, err)
	}

	err := mut.Checkpoint(ctx)
	require.NoError(t, err)

	for i, ed := range post {
		err = mut.Put(ctx, ed[0], ed[1])
		require.NoError(t, err)

		// flush post-checkpoint edits halfway through
		// this creates a stashed tree in |mut|
		if i == len(post)/2 {
			err = mut.flushPending(ctx)
			require.NoError(t, err)
		}
	}

	for _, ed := range edits {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.True(t, ok)
	}

	mut.Revert(ctx)

	for _, ed := range pre {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.True(t, ok)
	}
	for _, ed := range post {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.False(t, ok)
	}
}

// testRevertAfterFlushWithoutCheckpointedEdits reverts writes that were flushed when no checkpointed writes were
// pending, either because none were ever made or because they were flushed before the checkpoint was taken.
func testRevertAfterFlushWithoutCheckpointedEdits(t *testing.T, scale int) {
	ctx := context.Background()
	for _, flushBeforeCheckpoint := range []bool{false, true} {
		m := ascendingIntMapWithStep(t, scale, 2)
		mut := checkInvariants(t, m.Mutate())

		edits := ascendingTuplesWithStepAndStart(scale/5, 2, 1)
		pre, post := edits[:scale/10], edits[scale/10:]
		if !flushBeforeCheckpoint {
			pre, post = nil, edits
		}

		for _, ed := range pre {
			require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
		}
		require.NoError(t, mut.flushPending(ctx))
		require.NoError(t, mut.Checkpoint(ctx))

		for i, ed := range post {
			require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
			if i == len(post)/2 {
				require.NoError(t, mut.flushPending(ctx))
			}
		}

		mut.Revert(ctx)

		for _, ed := range pre {
			ok, err := mut.Has(ctx, ed[0])
			require.NoError(t, err)
			assert.True(t, ok)
		}
		for _, ed := range post {
			ok, err := mut.Has(ctx, ed[0])
			require.NoError(t, err)
			assert.False(t, ok)
		}
	}
}

// testRevertTwiceAfterFlush reverts to a stashed checkpoint, then makes and reverts more writes.
func testRevertTwiceAfterFlush(t *testing.T, scale int) {
	ctx := context.Background()
	m := ascendingIntMapWithStep(t, scale, 2)
	mut := checkInvariants(t, m.Mutate())

	edits := ascendingTuplesWithStepAndStart(scale/5, 2, 1)
	pre, post := edits[:scale/10], edits[scale/10:]

	for _, ed := range pre {
		require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
	}
	require.NoError(t, mut.Checkpoint(ctx))

	for _, ed := range post {
		require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
	}
	require.NoError(t, mut.flushPending(ctx))
	mut.Revert(ctx)

	for _, ed := range post {
		require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
	}
	mut.Revert(ctx)

	for _, ed := range pre {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.True(t, ok)
	}
	for _, ed := range post {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.False(t, ok)
	}
}

// testRevertAfterTwoFlushes reverts after a second flush, which must not replace the checkpoint captured by the first.
func testRevertAfterTwoFlushes(t *testing.T, scale int) {
	ctx := context.Background()
	for _, checkpointedEdits := range []bool{false, true} {
		m := ascendingIntMapWithStep(t, scale, 2)
		mut := checkInvariants(t, m.Mutate())

		edits := ascendingTuplesWithStepAndStart(scale/5, 2, 1)
		pre, post := edits[:scale/10], edits[scale/10:]
		if !checkpointedEdits {
			pre, post = nil, edits
		}

		for _, ed := range pre {
			require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
		}
		require.NoError(t, mut.Checkpoint(ctx))

		half := len(post) / 2
		for _, ed := range post[:half] {
			require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
		}
		require.NoError(t, mut.flushPending(ctx))
		for _, ed := range post[half:] {
			require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
		}
		require.NoError(t, mut.flushPending(ctx))

		mut.Revert(ctx)

		for _, ed := range pre {
			ok, err := mut.Has(ctx, ed[0])
			require.NoError(t, err)
			assert.True(t, ok)
		}
		for _, ed := range post {
			ok, err := mut.Has(ctx, ed[0])
			require.NoError(t, err)
			assert.False(t, ok)
		}
	}
}

// testVisitGCRootsFlushesCheckpoint checks that VisitGCRoots flushes checkpointed edits which an earlier flush left
// pending in |flushedCheckpoint|, and visits the resulting root.
func testVisitGCRootsFlushesCheckpoint(t *testing.T, scale int) {
	ctx := context.Background()
	m := ascendingIntMapWithStep(t, scale, 2)
	mut := checkInvariants(t, m.Mutate())

	edits := ascendingTuplesWithStepAndStart(scale/5, 2, 1)
	pre, post := edits[:scale/10], edits[scale/10:]

	for _, ed := range pre {
		require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
	}
	require.NoError(t, mut.Checkpoint(ctx))
	for _, ed := range post {
		require.NoError(t, mut.Put(ctx, ed[0], ed[1]))
	}
	require.NoError(t, mut.flushPending(ctx))
	require.NotNil(t, mut.flushedCheckpoint)
	require.Equal(t, len(pre), mut.flushedCheckpoint.Edits.Count())

	visited := make(map[hash.Hash]bool)
	require.NoError(t, mut.VisitGCRoots(ctx, func(h hash.Hash) bool {
		visited[h] = true
		return false
	}))
	assert.Equal(t, 0, mut.flushedCheckpoint.Edits.Count())
	assert.True(t, visited[mut.tuples.Static.GetRoot().HashOf()])
	assert.True(t, visited[mut.flushedCheckpoint.Static.GetRoot().HashOf()])

	mut.Revert(ctx)

	for _, ed := range pre {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.True(t, ok)
	}
	for _, ed := range post {
		ok, err := mut.Has(ctx, ed[0])
		require.NoError(t, err)
		assert.False(t, ok)
	}
}

// invariantCheckingMap asserts MutableMap's checkpoint invariants after every operation which can change them.
type invariantCheckingMap struct {
	*MutableMap
	t *testing.T
}

func checkInvariants(t *testing.T, mut *MutableMap) invariantCheckingMap {
	return invariantCheckingMap{MutableMap: mut, t: t}
}

func (m invariantCheckingMap) check() {
	if fc := m.flushedCheckpoint; fc != nil {
		require.False(m.t, m.tuples.Edits.HasCheckpointedEdits(), "checkpointed edits pending alongside flushedCheckpoint")
		require.NotSame(m.t, m.tuples.Edits, fc.Edits, "flushedCheckpoint aliases tuples.Edits")
	}
}

func (m invariantCheckingMap) Put(ctx context.Context, key, value val.Tuple) error {
	defer m.check()
	return m.MutableMap.Put(ctx, key, value)
}

func (m invariantCheckingMap) Checkpoint(ctx context.Context) error {
	defer m.check()
	return m.MutableMap.Checkpoint(ctx)
}

func (m invariantCheckingMap) Revert(ctx context.Context) {
	defer m.check()
	m.MutableMap.Revert(ctx)
}

func (m invariantCheckingMap) flushPending(ctx context.Context) error {
	defer m.check()
	return m.MutableMap.flushPending(ctx)
}

func (m invariantCheckingMap) VisitGCRoots(ctx context.Context, roots func(hash.Hash) bool) error {
	defer m.check()
	return m.MutableMap.VisitGCRoots(ctx, roots)
}
