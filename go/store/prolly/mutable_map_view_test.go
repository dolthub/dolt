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

package prolly

import (
	"context"
	"io"
	"math/rand"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var viewKeyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: false},
	val.Type{Enc: val.Int64Enc, Nullable: false},
)
var viewValDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: true},
)

func TestMutableMapCheckpointView(t *testing.T) {
	for _, maxPending := range []int{8, 64, defaultMaxPending} {
		for _, scale := range []int{0, 10, 100, 1000} {
			name := "max pending " + strconv.Itoa(maxPending) + " scale " + strconv.Itoa(scale)
			t.Run(name, func(t *testing.T) {
				testCheckpointView(t, rand.New(rand.NewSource(int64(scale*maxPending))), scale, maxPending)
			})
		}
	}
}

// testCheckpointView makes random edits to a MutableMap, checkpointing between rounds of edits, and checks that a
// MapView of the mutable map reads the same as the materialized map at the last checkpoint.
func testCheckpointView(t *testing.T, rnd *rand.Rand, scale, maxPending int) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()
	kb := val.NewTupleBuilder(viewKeyDesc, ns)
	vb := val.NewTupleBuilder(viewValDesc, ns)
	key := func(a, b int64) val.Tuple {
		kb.PutInt64(0, a)
		kb.PutInt64(1, b)
		k, err := kb.Build(ctx, ns.Pool())
		require.NoError(t, err)
		return k
	}
	value := func(v int64) val.Tuple {
		vb.PutInt64(0, v)
		tup, err := vb.Build(ctx, ns.Pool())
		require.NoError(t, err)
		return tup
	}

	// keys are (a, b) pairs with few distinct values of |a|, so that prefix lookups match many keys
	spread := int64(scale/4 + 2)
	randKey := func() val.Tuple {
		return key(rnd.Int63n(spread/4+1), rnd.Int63n(spread))
	}

	var tuples [][2]val.Tuple
	seen := make(map[string]bool)
	for i := 0; i < scale; i++ {
		k := randKey()
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		tuples = append(tuples, [2]val.Tuple{k, value(rnd.Int63())})
	}
	tree.SortTuplePairs(ctx, tuples, viewKeyDesc)
	m := mustProllyMapFromTuples(t, viewKeyDesc, viewValDesc, tuples, ns)
	mut := m.Mutate().WithMaxPending(maxPending)

	edit := func(n int) {
		for i := 0; i < n; i++ {
			if rnd.Intn(3) == 0 {
				require.NoError(t, mut.Delete(ctx, randKey()))
			} else {
				require.NoError(t, mut.Put(ctx, randKey(), value(rnd.Int63())))
			}
		}
	}

	for round := 0; round < 4; round++ {
		edit(scale/2 + 5)
		require.NoError(t, mut.Checkpoint(ctx))
		expected, err := mut.Map(ctx)
		require.NoError(t, err)

		view, err := CheckpointView(ctx, mut)
		require.NoError(t, err)
		checkViewMatches(t, ctx, view, expected, spread)

		// edits after the checkpoint are not visible to existing or new views
		edit(scale/2 + 5)
		checkViewMatches(t, ctx, view, expected, spread)
		view, err = CheckpointView(ctx, mut)
		require.NoError(t, err)
		checkViewMatches(t, ctx, view, expected, spread)

		// nor after the edits are reverted and replaced
		mut.Revert(ctx)
		edit(scale/2 + 5)
		checkViewMatches(t, ctx, view, expected, spread)
		mut.Revert(ctx)

		// a MutableMap made from the view has its contents, and its edits don't change the view
		restored := m.Mutate().WithMaxPending(maxPending)
		require.NoError(t, restored.Put(ctx, randKey(), value(rnd.Int63())))
		view.CopyTo(restored)
		actual, err := restored.Map(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected.HashOf(), actual.HashOf())
		require.NoError(t, restored.Put(ctx, randKey(), value(rnd.Int63())))
		require.NoError(t, restored.Delete(ctx, randKey()))
		checkViewMatches(t, ctx, view, expected, spread)
	}
}

func checkViewMatches(t *testing.T, ctx context.Context, view MapView, expected Map, spread int64) {
	ns := view.NodeStore()

	all, err := expected.IterAll(ctx)
	require.NoError(t, err)
	expTuples := collectIter(t, ctx, all)

	iter, err := view.IterAll(ctx)
	require.NoError(t, err)
	assert.Equal(t, expTuples, collectIter(t, ctx, iter))

	for _, kv := range expTuples {
		var actual val.Tuple
		require.NoError(t, view.Get(ctx, kv[0], func(_, v val.Tuple) error {
			actual = v
			return nil
		}))
		assert.Equal(t, kv[1], actual)
	}

	prefDesc := viewKeyDesc.PrefixDesc(1)
	pb := val.NewTupleBuilder(prefDesc, ns)
	kb := val.NewTupleBuilder(viewKeyDesc, ns)
	for a := int64(0); a <= spread/4+1; a++ {
		pb.PutInt64(0, a)
		prefix, err := pb.Build(ctx, ns.Pool())
		require.NoError(t, err)

		var expKey, actKey val.Tuple
		require.NoError(t, expected.GetPrefix(ctx, prefix, prefDesc, func(k, _ val.Tuple) error {
			expKey = k
			return nil
		}))
		require.NoError(t, view.GetPrefix(ctx, prefix, prefDesc, func(k, _ val.Tuple) error {
			actKey = k
			return nil
		}))
		assert.Equal(t, expKey, actKey)

		rng, err := PrefixRange(ctx, prefix, prefDesc)
		require.NoError(t, err)
		expIter, err := expected.IterRange(ctx, rng)
		require.NoError(t, err)
		actIter, err := view.IterRange(ctx, rng)
		require.NoError(t, err)
		assert.Equal(t, collectIter(t, ctx, expIter), collectIter(t, ctx, actIter))

		expIter, err = expected.IterRangeReverse(ctx, rng)
		require.NoError(t, err)
		actIter, err = view.IterRangeReverse(ctx, rng)
		require.NoError(t, err)
		assert.Equal(t, collectIter(t, ctx, expIter), collectIter(t, ctx, actIter))

		for _, b := range []int64{0, spread / 2} {
			kb.PutInt64(0, a)
			kb.PutInt64(1, b)
			start, err := kb.Build(ctx, ns.Pool())
			require.NoError(t, err)
			kb.PutInt64(0, a+1)
			kb.PutInt64(1, b)
			stop, err := kb.Build(ctx, ns.Pool())
			require.NoError(t, err)

			// compare key ranges against a scan of |expected|
			for _, bounds := range [][2]val.Tuple{{start, stop}, {nil, stop}, {start, nil}} {
				var exp [][2]val.Tuple
				for _, kv := range expTuples {
					if bounds[0] != nil {
						if cmp, err := viewKeyDesc.Compare(ctx, kv[0], bounds[0]); err != nil || cmp < 0 {
							continue
						}
					}
					if bounds[1] != nil {
						if cmp, err := viewKeyDesc.Compare(ctx, kv[0], bounds[1]); err != nil || cmp >= 0 {
							continue
						}
					}
					exp = append(exp, kv)
				}
				actIter, err = view.IterKeyRange(ctx, bounds[0], bounds[1])
				require.NoError(t, err)
				assert.Equal(t, exp, collectIter(t, ctx, actIter))
			}

			for _, rng := range []Range{greaterOrEqualRange(start, viewKeyDesc), lesserRange(stop, viewKeyDesc)} {
				expIter, err = expected.IterRange(ctx, rng)
				require.NoError(t, err)
				actIter, err = view.IterRange(ctx, rng)
				require.NoError(t, err)
				assert.Equal(t, collectIter(t, ctx, expIter), collectIter(t, ctx, actIter))

				// compare against the reversed forward iteration of |expected|
				expIter, err = expected.IterRange(ctx, rng)
				require.NoError(t, err)
				exp := collectIter(t, ctx, expIter)
				slices.Reverse(exp)
				actIter, err = view.IterRangeReverse(ctx, rng)
				require.NoError(t, err)
				assert.Equal(t, exp, collectIter(t, ctx, actIter))
			}
		}
	}
}

func collectIter(t *testing.T, ctx context.Context, iter MapIter) [][2]val.Tuple {
	var tuples [][2]val.Tuple
	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			return tuples
		}
		require.NoError(t, err)
		tuples = append(tuples, [2]val.Tuple{k, v})
	}
}
