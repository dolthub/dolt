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
		})
	}
}

func testCheckpoint(t *testing.T, scale int) {
	// create map with |s| even int64s
	ctx := context.Background()
	m := ascendingIntMapWithStep(t, scale, 2)
	mut := m.Mutate()

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
	mut := m.Mutate()

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
	mut := m.Mutate()

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
			err = mut.flushPending(ctx, false)
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
