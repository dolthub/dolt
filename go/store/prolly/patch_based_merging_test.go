// Copyright 2025 Dolthub, Inc.
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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// getChunkBoundaries returns all the keys in a node, which indicate the chunk boundaries in the previous tree level.
func getChunkBoundaries(t *testing.T, desc val.TupleDesc, nd tree.Node) (boundaries []uint32) {
	for i := 0; i < nd.Count(); i++ {
		key := nd.GetKey(i)
		keyInt, ok := desc.GetUint32(0, val.Tuple(key))
		require.True(t, ok, "failed to get int key from node")
		boundaries = append(boundaries, keyInt)
	}
	return boundaries
}

// slicePatchIter implements tree.PatchIter and is used in tests to inspect a list of patches from a RangeDiffer
// before applying them.
type slicePatchIter struct {
	slice []tree.Patch
	index int
}

var _ tree.PatchIter = &slicePatchIter{}

func (s *slicePatchIter) NextPatch(ctx context.Context) (tree.Patch, error) {
	if s.index >= len(s.slice) {
		return tree.Patch{}, nil
	}
	result := s.slice[s.index]
	s.index++
	return result, nil
}

func (s *slicePatchIter) Close() error {
	return nil
}

// producePatches produces a set of patches that can be applied to the left tree to produce the result of a three way merge.
// It passes the produced PatchIter to the callback function.
func producePatches[K ~[]byte, O tree.Ordering[K]](
	ctx context.Context,
	ns tree.NodeStore,
	left, right, base tree.Node,
	collide tree.CollisionFn,
	order O,
	cb func(buffer tree.PatchIter) error,
) (err error) {
	ld, err := tree.PatchGeneratorFromRoots[K](ctx, ns, ns, base, left, order)
	if err != nil {
		return err
	}

	rd, err := tree.PatchGeneratorFromRoots[K](ctx, ns, ns, base, right, order)
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	patches := tree.NewPatchBuffer(tree.PatchBufferSize)

	// One goroutine produces the patches, the other consumes them.
	eg.Go(func() (err error) {
		defer func() {
			if cerr := patches.Close(); err == nil {
				err = cerr
			}
		}()
		err = tree.SendPatches(ctx, ld, rd, patches, collide)
		return
	})

	eg.Go(func() error {
		return cb(patches)
	})

	if err = eg.Wait(); err != nil {
		return err
	}

	return nil
}

// mutation is a description of a single key-value change, used for tests to describe changes made to each branch.
type mutation[T interface{}] struct {
	key   T
	value *T
}

func update[T interface{}](key, value T) mutation[T] {
	return mutation[T]{key, &value}
}

func remove[T interface{}](key T) mutation[T] {
	return mutation[T]{key, nil}
}

// expectedPatch is a description of a patch returned by the patch generator. It's used in tests to describe expected
// output.
type expectedPatch[T interface{}] struct {
	startKey, endKey T
	noStartKey       bool
	level            int
	subtreeCount     uint64
	toValue          T
	isRemoval        bool
}

func pointUpdate[T interface{}](key, value T) expectedPatch[T] {
	return expectedPatch[T]{
		endKey:       key,
		noStartKey:   false,
		level:        0,
		subtreeCount: 1,
		toValue:      value,
		isRemoval:    false,
	}
}

func pointRemove[T interface{}](key T) expectedPatch[T] {
	return expectedPatch[T]{
		endKey:     key,
		noStartKey: false,
		isRemoval:  true,
	}
}

func manyPointRemoves(keyStart, keyEnd uint32) (patches []expectedPatch[uint32]) {
	for i := keyStart; i <= keyEnd; i++ {
		patches = append(patches, pointRemove(i))
	}
	return patches
}

func manyPointInserts(keyStart, keyEnd uint32) (patches []expectedPatch[uint32]) {
	for i := keyStart; i <= keyEnd; i++ {
		patches = append(patches, pointUpdate(i, i))
	}
	return patches
}

// testPatchBasedMergingResultsOnly checks that the result of a merge is the same with both the original merge
// algorithm and the tree-based merge, but it skips asserting that the set of produced patches matches what's expected.
// We use this when there's known issues where we produce sub-optimal patches but we still want to check correctness.
func testPatchBasedMergingResultsOnly[T interface{}](
	t *testing.T,
	ctx context.Context,
	ns tree.NodeStore,
	keyDesc val.TupleDesc,
	collide tree.CollisionFn,
	baseRoot, leftRoot, rightRoot tree.Node,
	expectedPatches []expectedPatch[T],
) {
	serializer := message.NewProllyMapSerializer(keyDesc, ns.Pool())
	traditionalMergeRoot, err := traditionalThreeWayMerge(ctx, ns, leftRoot, rightRoot, baseRoot, collide, false, false, keyDesc, serializer)
	patchBasedMergeRoot, _, err := tree.ThreeWayMerge(ctx, ns, leftRoot, rightRoot, baseRoot, collide, keyDesc, serializer)
	require.NoError(t, err)
	assert.Equal(t, patchBasedMergeRoot, traditionalMergeRoot)

}

func testPatchBasedMerging[T interface{}](t *testing.T, ctx context.Context, ns tree.NodeStore, keyDesc val.TupleDesc, collide tree.CollisionFn, baseRoot, leftRoot, rightRoot tree.Node, expectedPatches []expectedPatch[T]) {
	err := producePatches(ctx, ns, leftRoot, rightRoot, baseRoot, collide, keyDesc, func(iter tree.PatchIter) error {
		var actualPatches []tree.Patch
		actualPatch, err := iter.NextPatch(ctx)
		require.NoError(t, err)
		for actualPatch.EndKey != nil {
			actualPatches = append(actualPatches, actualPatch)
			actualPatch, err = iter.NextPatch(ctx)
			require.NoError(t, err)
		}

		assert.Equal(t, len(expectedPatches), len(actualPatches), "expected %d patches but found %d", len(expectedPatches), len(actualPatches))
		for i, actualPatch := range actualPatches {
			expectedPatch := expectedPatches[i]
			require.Equal(t, expectedPatch.level, actualPatch.Level, "patch %d has unexpected level. Expected %d, found %d", i, expectedPatch.level, actualPatch.Level)
			if expectedPatch.noStartKey || expectedPatch.level == 0 {
				assert.Nil(t, actualPatch.KeyBelowStart)
			} else {
				require.NotNil(t, actualPatch.KeyBelowStart, "patch %d has unexpected start key. Expected %d, found nil", i, expectedPatch.startKey)
				if keyDesc.Types[0].Enc == val.Uint32Enc {
					actualStartKey, _ := keyDesc.GetUint32(0, val.Tuple(actualPatch.KeyBelowStart))
					assert.Equal(t, expectedPatch.startKey, actualStartKey, "patch %d has unexpected start key. Expected %d, found %d", i, expectedPatch.startKey, actualStartKey)
				} else {
					actualStartKey, _ := keyDesc.GetString(0, val.Tuple(actualPatch.KeyBelowStart))
					assert.Equal(t, expectedPatch.startKey, actualStartKey, "patch %d has unexpected start key. Expected %s, found %s", i, expectedPatch.startKey, actualStartKey)
				}
			}

			if keyDesc.Types[0].Enc == val.Uint32Enc {
				actualEndKey, _ := keyDesc.GetUint32(0, val.Tuple(actualPatch.EndKey))
				assert.Equal(t, expectedPatch.endKey, actualEndKey, "patch %d has unexpected end key. Expected %d, found %d", i, expectedPatch.endKey, actualEndKey)
			} else {
				actualEndKey, _ := keyDesc.GetString(0, val.Tuple(actualPatch.EndKey))
				assert.Equal(t, expectedPatch.endKey, actualEndKey, "patch %d has unexpected end key. Expected %s, found %s", i, expectedPatch.endKey, actualEndKey)
			}
			if actualPatch.To == nil {
				assert.True(t, expectedPatch.isRemoval, "patch %d should be a removal patch, but it isn't.", i)
			} else {
				if actualPatch.Level > 0 {
					assert.Equal(t, expectedPatch.subtreeCount, actualPatch.SubtreeCount, "patch %d has unexpected subtree count. Expected %d, found %d", i, expectedPatch.subtreeCount, actualPatch.SubtreeCount)
					expectedAddress, ok, err := tree.GetAddressFromLevelAndKeyForTest(ctx, ns, rightRoot, actualPatch.Level, val.Tuple(actualPatch.EndKey), keyDesc)
					require.NoError(t, err)
					require.True(t, ok)
					assert.Equal(t, tree.Item(expectedAddress[:]), actualPatch.To)
				} else {
					if keyDesc.Types[0].Enc == val.Uint32Enc {
						actualToValue, _ := keyDesc.GetUint32(0, val.Tuple(actualPatch.To))
						assert.Equal(t, expectedPatch.toValue, actualToValue, "patch %d has unexpected to value. Expected %d, found %d", i, expectedPatch.toValue, actualToValue)
					} else {
						actualToValue, _ := keyDesc.GetString(0, val.Tuple(actualPatch.To))
						assert.Equal(t, expectedPatch.toValue, actualToValue, "patch %d has unexpected to value. Expected %s, found %s", i, expectedPatch.toValue, actualToValue)
					}
				}
			}
		}

		// In order to apply the patches, we need to wrap them in a new tree.PatchIter
		patchIter := slicePatchIter{slice: actualPatches}

		serializer := message.NewProllyMapSerializer(keyDesc, ns.Pool())
		// verify that this is equivalent to a traditional merge.
		traditionalMergeRoot, err := traditionalThreeWayMerge(ctx, ns, leftRoot, rightRoot, baseRoot, collide, false, false, keyDesc, serializer)
		mergedRoot, err := tree.ApplyPatches(ctx, ns, leftRoot, keyDesc, serializer, &patchIter)
		require.NoError(t, err)
		assert.Equal(t, mergedRoot, traditionalMergeRoot)

		return nil
	})
	require.NoError(t, err)
}

// mutate creates a new tree map by applying a sequence of mutations to an existing tree map.
func mutate(t *testing.T, ctx context.Context, baseRoot tree.Node, keyDesc, valDesc val.TupleDesc, mutations []mutation[uint32]) tree.Node {
	baseMap := NewMap(baseRoot, ns, keyDesc, valDesc)

	keyBld := val.NewTupleBuilder(keyDesc, ns)
	valBld := val.NewTupleBuilder(valDesc, ns)
	var err error

	mutMap := baseMap.Mutate()
	for _, m := range mutations {
		keyBld.PutUint32(0, m.key)
		var newValue val.Tuple
		if m.value != nil {
			valBld.PutUint32(0, *m.value)
			newValue, err = valBld.Build(sharedPool)
			require.NoError(t, err)
		}
		newKey, err := keyBld.Build(sharedPool)
		require.NoError(t, err)

		err = mutMap.Put(ctx, newKey, newValue)
		require.NoError(t, err)
	}
	newMap, err := mutMap.Map(ctx)
	require.NoError(t, err)
	return newMap.Node()
}

func mutateStrings(t *testing.T, ctx context.Context, baseRoot tree.Node, keyDesc, valDesc val.TupleDesc, mutations []mutation[string]) tree.Node {
	baseMap := NewMap(baseRoot, ns, keyDesc, valDesc)

	keyBld := val.NewTupleBuilder(keyDesc, ns)
	valBld := val.NewTupleBuilder(valDesc, ns)
	var err error

	mutMap := baseMap.Mutate()
	for _, m := range mutations {
		err := keyBld.PutString(0, m.key)
		require.NoError(t, err)
		var newValue val.Tuple
		if m.value != nil {
			err = valBld.PutString(0, *m.value)
			require.NoError(t, err)
			newValue, err = valBld.Build(sharedPool)
			require.NoError(t, err)
		}
		newKey, err := keyBld.Build(sharedPool)
		require.NoError(t, err)

		err = mutMap.Put(ctx, newKey, newValue)
		require.NoError(t, err)
	}
	newMap, err := mutMap.Map(ctx)
	require.NoError(t, err)
	return newMap.Node()
}

// makeDeletePatches produces mutations describing deleting all keys in a range.
func makeDeletePatches(start, stop uint32) (mutations []mutation[uint32]) {
	for i := start; i <= stop; i++ {
		mutations = append(mutations, remove(i))
	}
	return mutations
}

func makeSimpleIntMap(t *testing.T, start, stop int) (tree.Node, val.TupleDesc) {
	tups, desc := tree.AscendingUintTuplesWithStep(stop-start+1, start, start, 1)
	root, err := tree.MakeTreeForTest(tups)
	require.NoError(t, err)
	return root, desc
}

// TestPatchBasedMerging tests that the patching process produces the minimal set of patches which must be applied
// to the left tree in order to produce the merged tree.
func TestPatchBasedMerging(t *testing.T) {

	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	emptyMap, err := tree.MakeTreeForTest(nil)
	require.NoError(t, err)
	fourAndAHalfChunks, desc := makeSimpleIntMap(t, 1, 1250)
	threeAndAHalfChunks, _ := makeSimpleIntMap(t, 1, 1024)
	threeChunks, _ := makeSimpleIntMap(t, 1, 830)
	twoAndAHalfChunks, _ := makeSimpleIntMap(t, 1, 800)
	// The base map will happen to have these chunk boundaries.
	chunkBoundaries := []uint32{414, 718, 830, 1207}
	maxKey := uint32(1024)
	stringDesc := val.NewTupleDescriptor(val.Type{Enc: val.StringEnc})

	mapWithUpdates := func(root tree.Node, updates ...mutation[uint32]) tree.Node {
		return mutate(t, ctx, root, desc, desc, updates)
	}

	stringMapWithUpdates := func(root tree.Node, updates ...mutation[string]) tree.Node {
		return mutateStrings(t, ctx, root, stringDesc, stringDesc, updates)
	}

	t.Run("concurrent updates in same leaf node produce level 0 patch", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 0)),
			mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[0]-4, 1)),
			[]expectedPatch[uint32]{pointUpdate(chunkBoundaries[0]-4, 1)})
	})

	t.Run("update in first child node produces top-level patch with no start key", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](1000, 1)),
			mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[0]-10, 1)),
			[]expectedPatch[uint32]{
				{
					noStartKey:   true,
					endKey:       chunkBoundaries[0],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[0]),
				},
			})
	})

	t.Run("update in middle child node produces top-level patch with start key", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[2]-50, 1)),
			[]expectedPatch[uint32]{
				{
					startKey:     chunkBoundaries[1],
					endKey:       chunkBoundaries[2],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
				},
			})
	})

	t.Run("update in final child node produces leaf patch (a top-level patch would not end on a proper chunk boundary)", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update[uint32](maxKey-100, 1)),
			[]expectedPatch[uint32]{
				{
					level:   0,
					endKey:  maxKey - 100,
					toValue: 1,
				},
			})
	})

	t.Run("an insert beyond the final key doesn't create an extra chunk boundary", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](0, 1)),
			mapWithUpdates(threeAndAHalfChunks, update[uint32](maxKey+1, 1)),
			[]expectedPatch[uint32]{
				{
					level:   0,
					endKey:  maxKey + 1,
					toValue: 1,
				},
			})
	})

	t.Run("an insert before the first key doesn't create an extra chunk boundary", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](maxKey+1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update[uint32](0, 1)),
			[]expectedPatch[uint32]{
				{
					noStartKey:   true,
					endKey:       chunkBoundaries[0],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[0] + 1),
				},
			})
	})

	t.Run("removing a value from a leaf node produces a top-level patch", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](maxKey-20)),
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)),
			[]expectedPatch[uint32]{
				{
					noStartKey:   true,
					endKey:       chunkBoundaries[0],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[0] - 1),
				},
			})
	})

	t.Run("concurrent removals in a leaf node produces a leaf-level patch", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)),
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](2)),
			[]expectedPatch[uint32]{
				{
					endKey:    2,
					level:     0,
					isRemoval: true,
				},
			})
	})

	t.Run("deleting an entire chunk at the end produces a single range", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)),
			mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(chunkBoundaries[2]+1, maxKey)...),
			[]expectedPatch[uint32]{
				{
					startKey:  chunkBoundaries[2],
					endKey:    maxKey,
					level:     1,
					isRemoval: true,
				},
			})
	})

	// We expect removing an entire chunk produces a single range covering the removed chunk and the subsequent one.
	t.Run("deleting an entire chunk in the middle", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)),
			mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(chunkBoundaries[0]+1, chunkBoundaries[1])...),
			[]expectedPatch[uint32]{
				{
					startKey:     chunkBoundaries[0],
					endKey:       chunkBoundaries[2],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
				},
			})
	})

	t.Run("deleting an entire chunk concurrent with a point delete", func(t *testing.T) {
		// This needs to recurse into the leaf node in order to verify that the concurrently modified key isn't a conflict.
		// Once we confirm there's no conflict, we could potentially emit a single high-level patch, but the logic would be
		// more complicated. So currently we emit multiple patches instead.
		pointDelete := mapWithUpdates(threeAndAHalfChunks, remove[uint32](1000))
		chunkDelete := mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(831, 1024)...)
		expectedPatches := manyPointRemoves(831, 999)
		expectedPatches = append(expectedPatches, manyPointRemoves(1001, 1024)...)
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			pointDelete,
			chunkDelete,
			expectedPatches)
		// When the left side contains the larger change, the merged tree is the same as the left tree and there
		// are no patches to emit.
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			chunkDelete,
			pointDelete,
			[]expectedPatch[uint32]{})
	})

	t.Run("deleting many rows at the end", func(t *testing.T) {
		// We expect the chunk before the deleted rows to be modified, and emit then a removal patch for each chunk until the end.
		// Again, we could potentially emit a single removal patch, or even a single modified patch, but the logic would be
		// more complicated.
		newMaxKey := chunkBoundaries[2] - 1
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)),
			mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(newMaxKey+1, maxKey)...),
			[]expectedPatch[uint32]{
				{
					level:     0,
					endKey:    chunkBoundaries[2],
					isRemoval: true,
				},
				{
					startKey:  chunkBoundaries[2],
					endKey:    1024,
					level:     1,
					isRemoval: true,
				},
			})
	})

	t.Run("deleting many rows at the end doesn't emit unnecessary leaf removals", func(t *testing.T) {
		newMaxKey := chunkBoundaries[2] - 1
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove[uint32](818)),
			mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(newMaxKey+1, maxKey)...),
			[]expectedPatch[uint32]{
				{
					endKey:       chunkBoundaries[2],
					level:        0,
					subtreeCount: uint64(newMaxKey - chunkBoundaries[1]),
					isRemoval:    true,
				},
				{
					startKey:  chunkBoundaries[2],
					endKey:    1024,
					level:     1,
					isRemoval: true,
				},
			})
	})

	t.Run("deleting many rows in the middle doesn't emit unnecessary leaf removals", func(t *testing.T) {
		var expectedPatches []expectedPatch[uint32]
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			level:        1,
			startKey:     chunkBoundaries[0],
			endKey:       1237,
			subtreeCount: uint64(1237 - chunkBoundaries[0] - (1210 - 715) - 1),
		})
		testPatchBasedMerging(t, ctx, ns, desc, nil,
			fourAndAHalfChunks,
			mapWithUpdates(fourAndAHalfChunks, remove[uint32](100)),
			mapWithUpdates(fourAndAHalfChunks, makeDeletePatches(715, 1210)...),
			expectedPatches)
	})

	t.Run("deleting an chunk boundary produces new boundaries until they realign", func(t *testing.T) {
		newChunkBoundary := uint32(436)
		testPatchBasedMerging(t, ctx, ns, desc, nil, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1000)), mapWithUpdates(threeAndAHalfChunks, remove[uint32](chunkBoundaries[0])), []expectedPatch[uint32]{
			{
				noStartKey:   true,
				endKey:       newChunkBoundary, // new chunk boundary
				level:        1,
				subtreeCount: uint64(newChunkBoundary - 1),
			},
			{
				startKey:     newChunkBoundary,
				endKey:       chunkBoundaries[1],
				level:        1,
				subtreeCount: uint64(chunkBoundaries[1] - newChunkBoundary),
			},
		})
	})

	t.Run("range insert at end", func(t *testing.T) {
		// The last chunk from the base is replaced, a new chunk is added, and the remaining rows,
		// which don't end in a natural chunk boundary, get emitted as individual rows.
		var expectedPatches []expectedPatch[uint32]
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			startKey:     chunkBoundaries[1],
			endKey:       chunkBoundaries[2],
			level:        1,
			subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
		})
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			startKey:     chunkBoundaries[2],
			endKey:       chunkBoundaries[3],
			level:        1,
			subtreeCount: uint64(chunkBoundaries[3] - chunkBoundaries[2]),
		})
		for i := chunkBoundaries[3] + 1; i <= 1250; i++ {
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				endKey:  i,
				toValue: i,
				level:   0,
			})
		}
		testPatchBasedMerging(t, ctx, ns, desc, nil, twoAndAHalfChunks, mapWithUpdates(twoAndAHalfChunks, update[uint32](1, 0)), fourAndAHalfChunks, expectedPatches)
	})

	t.Run("non-overlapping inserts to empty map", func(t *testing.T) {
		lowerMap, _ := makeSimpleIntMap(t, 1, 831)
		upperMap, _ := makeSimpleIntMap(t, 1001, 1847)
		upperMapChunkBoundary := []uint32{1237, 1450, 1846}
		{
			// The right map is the upper map.
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			// Once we hit the first chunk boundary, we emit higher level patches.
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1001, upperMapChunkBoundary[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[0],
				endKey:       upperMapChunkBoundary[1],
				subtreeCount: uint64(upperMapChunkBoundary[1] - upperMapChunkBoundary[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[1],
				endKey:       upperMapChunkBoundary[2],
				subtreeCount: uint64(upperMapChunkBoundary[2] - upperMapChunkBoundary[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})

			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, lowerMap, upperMap, expectedPatches)
		}
		{
			// The right map is the lower map.
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			// Once we hit the first chunk boundary, we emit higher level patches.
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1, chunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[0],
				endKey:       chunkBoundaries[1],
				subtreeCount: uint64(chunkBoundaries[1] - chunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[1],
				endKey:       chunkBoundaries[2],
				subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, pointUpdate[uint32](831, 831))
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		}
	})

	t.Run("non-overlapping inserts to nearly empty map", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1000, 1000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 831)
		upperMap, _ := makeSimpleIntMap(t, 1001, 1847)
		upperMapChunkBoundary := []uint32{1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			// Once we hit the first chunk boundary, we emit higher level patches.
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1001, upperMapChunkBoundary[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[0],
				endKey:       upperMapChunkBoundary[1],
				subtreeCount: uint64(upperMapChunkBoundary[1] - upperMapChunkBoundary[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[1],
				endKey:       upperMapChunkBoundary[2],
				subtreeCount: uint64(upperMapChunkBoundary[2] - upperMapChunkBoundary[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			// Once we hit the first chunk boundary, we emit higher level patches.
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1, chunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[0],
				endKey:       chunkBoundaries[1],
				subtreeCount: uint64(chunkBoundaries[1] - chunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[1],
				endKey:       chunkBoundaries[2],
				subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, pointUpdate[uint32](831, 831))
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("overlapping inserts to empty map", func(t *testing.T) {
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		upperMap, _ := makeSimpleIntMap(t, 751, 1847)
		upperMapChunkBoundary := []uint32{1012, 1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1001, upperMapChunkBoundary[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[0],
				endKey:       upperMapChunkBoundary[1],
				subtreeCount: uint64(upperMapChunkBoundary[1] - upperMapChunkBoundary[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[1],
				endKey:       upperMapChunkBoundary[2],
				subtreeCount: uint64(upperMapChunkBoundary[2] - upperMapChunkBoundary[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[2],
				endKey:       upperMapChunkBoundary[3],
				subtreeCount: uint64(upperMapChunkBoundary[3] - upperMapChunkBoundary[2]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1, chunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[0],
				endKey:       chunkBoundaries[1],
				subtreeCount: uint64(chunkBoundaries[1] - chunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, manyPointInserts(chunkBoundaries[1]+1, 750)...)

			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to empty map: small before large", func(t *testing.T) {
		lowerMap, _ := makeSimpleIntMap(t, 1, 1)
		upperMap, _ := makeSimpleIntMap(t, 1001, 1847)
		upperMapChunkBoundaries := []uint32{1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			for i := uint32(1001); i <= upperMapChunkBoundaries[0]; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundaries[0],
				endKey:       upperMapChunkBoundaries[1],
				subtreeCount: uint64(upperMapChunkBoundaries[1] - upperMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundaries[1],
				endKey:       upperMapChunkBoundaries[2],
				subtreeCount: uint64(upperMapChunkBoundaries[2] - upperMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches = []expectedPatch[uint32]{
				pointUpdate[uint32](1, 1),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to nearly empty map: small insert, then base, then large insert", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 500, 500)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1)
		upperMap, _ := makeSimpleIntMap(t, 1001, 1847)
		rightMapChunkBoundaries := []uint32{1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			for i := uint32(1001); i <= rightMapChunkBoundaries[0]; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     rightMapChunkBoundaries[0],
				endKey:       rightMapChunkBoundaries[1],
				subtreeCount: uint64(rightMapChunkBoundaries[1] - rightMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     rightMapChunkBoundaries[1],
				endKey:       rightMapChunkBoundaries[2],
				subtreeCount: uint64(rightMapChunkBoundaries[2] - rightMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches = []expectedPatch[uint32]{
				pointUpdate[uint32](1, 1),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to nearly empty map: base, then small insert, then large insert", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 0, 0)
		lowerMap, _ := makeSimpleIntMap(t, 500, 500)
		upperMap, _ := makeSimpleIntMap(t, 1001, 1847)
		upperMapChunkBoundaries := []uint32{1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1001, upperMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundaries[0],
				endKey:       upperMapChunkBoundaries[1],
				subtreeCount: uint64(upperMapChunkBoundaries[1] - upperMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundaries[1],
				endKey:       upperMapChunkBoundaries[2],
				subtreeCount: uint64(upperMapChunkBoundaries[2] - upperMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches = []expectedPatch[uint32]{
				pointUpdate[uint32](500, 500),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to empty map: small after large", func(t *testing.T) {
		upperMap, _ := makeSimpleIntMap(t, 2000, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		lowerMapChunkBoundaries := []uint32{414, 718, 830}
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				noStartKey:   true,
				endKey:       lowerMapChunkBoundaries[0],
				subtreeCount: uint64(lowerMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[0],
				endKey:       lowerMapChunkBoundaries[1],
				subtreeCount: uint64(lowerMapChunkBoundaries[1] - lowerMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[1],
				endKey:       lowerMapChunkBoundaries[2],
				subtreeCount: uint64(lowerMapChunkBoundaries[2] - lowerMapChunkBoundaries[1]),
			})

			for i := lowerMapChunkBoundaries[2] + 1; i <= 1000; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}

			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		})
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches = []expectedPatch[uint32]{
				pointUpdate[uint32](2000, 2000),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, lowerMap, upperMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to nearly empty map: large insert, then base, then small insert", func(t *testing.T) {
		// The right map will need to recurse down to the leaf level to verify that the key at 500 is not modified.
		// This is a place where we could then emit a range for the entire node once we verify that all changes are
		// concurrent, but we currently don't.
		baseMap, _ := makeSimpleIntMap(t, 900, 900)
		upperMap, _ := makeSimpleIntMap(t, 2000, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 831)
		lowerMapChunkBoundaries := []uint32{414, 718, 830}
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				noStartKey:   true,
				endKey:       lowerMapChunkBoundaries[0],
				subtreeCount: uint64(lowerMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[0],
				endKey:       lowerMapChunkBoundaries[1],
				subtreeCount: uint64(lowerMapChunkBoundaries[1] - lowerMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[1],
				endKey:       lowerMapChunkBoundaries[2],
				subtreeCount: uint64(lowerMapChunkBoundaries[2] - lowerMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, pointUpdate[uint32](831, 831))
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches = []expectedPatch[uint32]{
				pointUpdate[uint32](2000, 2000),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to nearly empty map: base, then large insert, then small insert", func(t *testing.T) {
		// The right map will need to recurse down to the leaf level to verify that the key at 500 is not modified.
		// This is a place where we could then emit a range for the entire node once we verify that all changes are
		// concurrent, but we currently don't.
		baseMap, _ := makeSimpleIntMap(t, 0, 0)
		upperMap, _ := makeSimpleIntMap(t, 2000, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 831)
		lowerMapChunkBoundaries := []uint32{414, 718, 830}
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1, lowerMapChunkBoundaries[0])...)

			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[0],
				endKey:       lowerMapChunkBoundaries[1],
				subtreeCount: uint64(lowerMapChunkBoundaries[1] - lowerMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[1],
				endKey:       lowerMapChunkBoundaries[2],
				subtreeCount: uint64(lowerMapChunkBoundaries[2] - lowerMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  831,
				toValue: 831,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
		t.Run("right map is the upper map", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{
				pointUpdate[uint32](2000, 2000),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to nearly empty map: large insert, then small insert, then base", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 3000, 3000)
		upperMap, _ := makeSimpleIntMap(t, 2000, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 831)
		rightMapChunkBoundaries := []uint32{414, 718, 830}
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				noStartKey:   true,
				endKey:       rightMapChunkBoundaries[0],
				subtreeCount: uint64(rightMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     rightMapChunkBoundaries[0],
				endKey:       rightMapChunkBoundaries[1],
				subtreeCount: uint64(rightMapChunkBoundaries[1] - rightMapChunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     rightMapChunkBoundaries[1],
				endKey:       rightMapChunkBoundaries[2],
				subtreeCount: uint64(rightMapChunkBoundaries[2] - rightMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  831,
				toValue: 831,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
		t.Run("right map is the upper map", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{
				pointUpdate[uint32](2000, 2000),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to nearly empty map: large insert concurrent with base, then small insert", func(t *testing.T) {
		// The right map will need to recurse down to the leaf level to verify that the key at 500 is not modified.
		// This is a place where we could then emit a range for the entire node once we verify that all changes are
		// concurrent, but we currently don't.
		baseMap, _ := makeSimpleIntMap(t, 500, 500)
		upperMap := mapWithUpdates(baseMap, update[uint32](2000, 2000))
		lowerMap, _ := makeSimpleIntMap(t, 1, 831)
		lowerMapChunkBoundaries := []uint32{414, 718, 830}
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				noStartKey:   true,
				endKey:       lowerMapChunkBoundaries[0],
				subtreeCount: uint64(lowerMapChunkBoundaries[0]),
			})
			// We're able to emit a level 1 patch that covers the left key 500, because there's no diff on the left
			// for that key; this is safe.
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[0],
				endKey:       lowerMapChunkBoundaries[1],
				subtreeCount: uint64(lowerMapChunkBoundaries[1] - lowerMapChunkBoundaries[0]),
			})

			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     lowerMapChunkBoundaries[1],
				endKey:       lowerMapChunkBoundaries[2],
				subtreeCount: uint64(lowerMapChunkBoundaries[2] - lowerMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  831,
				toValue: 831,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
		t.Run("right map is the upper map", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{
				pointUpdate[uint32](2000, 2000),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to empty map: concurrent", func(t *testing.T) {
		subsetMap, _ := makeSimpleIntMap(t, 500, 500)
		supersetMap, _ := makeSimpleIntMap(t, 1, 831)
		supersetMapChunkBoundaries := []uint32{414, 718, 830}
		t.Run("right map is superset", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				noStartKey:   true,
				endKey:       supersetMapChunkBoundaries[0],
				subtreeCount: uint64(supersetMapChunkBoundaries[0]),
			})
			for i := uint32(415); i < 500; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}
			for i := uint32(501); i <= supersetMapChunkBoundaries[1]; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     supersetMapChunkBoundaries[1],
				endKey:       supersetMapChunkBoundaries[2],
				subtreeCount: uint64(supersetMapChunkBoundaries[2] - supersetMapChunkBoundaries[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  831,
				toValue: 831,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, subsetMap, supersetMap, expectedPatches)
		})
		t.Run("right map is subset", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{}
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, supersetMap, subsetMap, expectedPatches)
		})
	})

	t.Run("unequal sized removals produce empty map: small removal less than large removal", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1)
		upperMap, _ := makeSimpleIntMap(t, 2, 2000)
		upperMapChunkBoundaries := getChunkBoundaries(t, desc, upperMap)
		t.Run("right side removes single row", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{pointRemove[uint32](1)}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right side removes many rows", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(2, upperMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[0],
				endKey:    upperMapChunkBoundaries[1],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[1],
				endKey:    upperMapChunkBoundaries[2],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[2],
				endKey:    upperMapChunkBoundaries[3],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[3],
				endKey:    upperMapChunkBoundaries[4],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[4],
				endKey:    upperMapChunkBoundaries[5],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[5],
				endKey:    2000,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})

	})

	t.Run("unequal sized removals produce nearly empty map: small removal less than large removal", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1)
		upperMap, _ := makeSimpleIntMap(t, 3, 2000)
		upperMapChunkBoundaries := getChunkBoundaries(t, desc, upperMap)
		t.Run("right side removes single row", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{pointRemove[uint32](1)}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right side removes many rows", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(3, upperMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[0],
				endKey:    upperMapChunkBoundaries[1],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[1],
				endKey:    upperMapChunkBoundaries[2],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[2],
				endKey:    upperMapChunkBoundaries[3],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[3],
				endKey:    upperMapChunkBoundaries[4],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[4],
				endKey:    upperMapChunkBoundaries[5],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  upperMapChunkBoundaries[5],
				endKey:    2000,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})

	})

	t.Run("unequal sized removals produce empty map: concurrent", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 1000)
		mapWithPointRemoval := mapWithUpdates(baseMap, remove[uint32](750))
		rightMapChunkBoundaries := getChunkBoundaries(t, desc, mapWithPointRemoval)
		t.Run("right is empty", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:      1,
				noStartKey: true,
				endKey:     rightMapChunkBoundaries[0],
				isRemoval:  true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  rightMapChunkBoundaries[0],
				endKey:    rightMapChunkBoundaries[1],
				isRemoval: true,
			})
			// TODO: This can be improved by emitting a single modfied patch instead of many removed patches.
			expectedPatches = append(expectedPatches, manyPointRemoves(rightMapChunkBoundaries[1]+1, 749)...)
			expectedPatches = append(expectedPatches, manyPointRemoves(751, rightMapChunkBoundaries[2])...)

			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  rightMapChunkBoundaries[2],
				endKey:    1000,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, mapWithPointRemoval, emptyMap, expectedPatches)
		})
		t.Run("left is empty", func(t *testing.T) {
			// In this case, the left contains a superset of the changes from the right.
			// The merge is equal to leftMap, with no patches necessary.
			expectedPatches := []expectedPatch[uint32]{}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, emptyMap, mapWithPointRemoval, expectedPatches)
		})

	})

	t.Run("unequal sized removals produce nearly empty map: concurrent", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 1000)
		mapWithPointRemoval := mapWithUpdates(baseMap, remove[uint32](750))
		nearlyEmptyMap, _ := makeSimpleIntMap(t, 1, 1)
		rightMapChunkBoundaries := getChunkBoundaries(t, desc, mapWithPointRemoval)
		t.Run("right is nearly empty", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(2, rightMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  rightMapChunkBoundaries[0],
				endKey:    rightMapChunkBoundaries[1],
				isRemoval: true,
			})
			// TODO: This can be improved by emitting a single modfied patch instead of many removed patches.
			expectedPatches = append(expectedPatches, manyPointRemoves(rightMapChunkBoundaries[1]+1, 749)...)
			expectedPatches = append(expectedPatches, manyPointRemoves(751, rightMapChunkBoundaries[2])...)

			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  rightMapChunkBoundaries[2],
				endKey:    1000,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, mapWithPointRemoval, nearlyEmptyMap, expectedPatches)
		})
		t.Run("left is nearly empty", func(t *testing.T) {
			// In this case, the left contains a superset of the changes from the right.
			// The merge is equal to leftMap, with no patches necessary.
			expectedPatches := []expectedPatch[uint32]{}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, nearlyEmptyMap, mapWithPointRemoval, expectedPatches)
		})

	})

	t.Run("merge with resolvable conflicts", func(t *testing.T) {
		valueOnCollision := uint32(3000)
		collide := func(left, right tree.Diff) (tree.Diff, bool) {
			tupleBuilder := val.NewTupleBuilder(desc, ns)
			tupleBuilder.PutUint32(0, valueOnCollision)
			newVal, err := tupleBuilder.Build(sharedPool)
			require.NoError(t, err)
			// Resolve conflicts by returning a special value that we check for
			return tree.Diff{
				Key:  left.Key,
				From: left.From,
				To:   tree.Item(newVal),
				Type: tree.ModifiedDiff,
			}, true
		}
		testPatchBasedMerging(t, ctx, ns, desc, collide, threeChunks, mapWithUpdates(threeChunks, update[uint32](1, 10), update[uint32](990, 0)), mapWithUpdates(threeChunks, update[uint32](1, 20), update[uint32](1000, 0)), []expectedPatch[uint32]{
			{
				endKey:  1,
				level:   0,
				toValue: valueOnCollision,
			},
			{
				endKey:  1000,
				level:   0,
				toValue: 0,
			},
		})
	})

	t.Run("merge with unresolvable conflicts", func(t *testing.T) {
		var conflicts []struct{ left, right tree.Diff }
		// A custom callback records all conflicts and treats them as unresolvable.
		collide := func(left, right tree.Diff) (tree.Diff, bool) {
			conflicts = append(conflicts, struct{ left, right tree.Diff }{left, right})
			return tree.Diff{}, false
		}
		testPatchBasedMerging(t, ctx, ns, desc, collide, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 10), update[uint32](500, 0)), mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 20), update[uint32](800, 0)), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[1],
				endKey:       chunkBoundaries[2],
				level:        1,
				subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
			},
		})
		// The callback will be called twice: once during the tree merge, and once during the traditional merge
		// we're comparing the results to.
		require.Len(t, conflicts, 2)
		for _, conflict := range conflicts {
			collisionKey, ok := desc.GetUint32(0, val.Tuple(conflict.left.Key))
			require.True(t, ok)
			assert.Equal(t, collisionKey, uint32(1))
		}
	})

	t.Run("both branches produce same address with different key ranges", func(t *testing.T) {
		// This is a corner case that can happen when one branch adds or removes an entire chunk, immediately before
		// a chunk that is concurrently modified.
		testPatchBasedMerging(t, ctx, ns, desc, nil, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[2]-30, 0)), mapWithUpdates(threeAndAHalfChunks, append(makeDeletePatches(chunkBoundaries[0]+1, chunkBoundaries[1]), update[uint32](chunkBoundaries[2]-30, 0))...), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[0],
				endKey:       chunkBoundaries[2],
				level:        1,
				subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
			},
		})
	})

	t.Run("multi-level tree: deleting many rows off the end produces minimal diffs", func(t *testing.T) {
		bld := val.NewTupleBuilder(stringDesc, nil)
		tuples := make([][2]val.Tuple, 10000)
		var err error
		for i := range tuples {
			err = bld.PutString(0, "long_string_key_goes_here_"+strconv.Itoa(i))
			require.NoError(t, err)
			tuples[i][0], err = bld.Build(sharedPool)
			require.NoError(t, err)
			err = bld.PutString(0, "long_string_value_goes_here_"+strconv.Itoa(i))
			require.NoError(t, err)
			tuples[i][1], err = bld.Build(sharedPool)
			require.NoError(t, err)
		}
		baseRoot, err := tree.MakeTreeForTest(tuples)
		require.NoError(t, err)
		leftRoot := stringMapWithUpdates(baseRoot, update[string]("long_string_key_goes_here_8659", ""))
		require.NoError(t, err)
		// This key is specifically chosen to be the last key of the last key of the last key:
		// Removing it should result in exactly three diffs, one at each level.
		rightRoot, err := tree.MakeTreeForTest(tuples[:8660])
		require.NoError(t, err)
		// The base map will happen to have these chunk boundaries.
		testPatchBasedMerging[string](t, ctx, ns, stringDesc, nil, baseRoot, leftRoot, rightRoot, []expectedPatch[string]{
			{
				endKey:    "long_string_key_goes_here_8660",
				level:     0,
				isRemoval: true,
			},
			{
				startKey:  "long_string_key_goes_here_8660",
				endKey:    "long_string_key_goes_here_8702",
				level:     1,
				isRemoval: true,
			},
			{
				startKey:  "long_string_key_goes_here_8702",
				endKey:    "long_string_key_goes_here_9999",
				level:     2,
				isRemoval: true,
			},
		})
	})
}

// TestPatchBasedMergingSkipCheckingPatches tests that patching process produces the same merged tree as a traditional merge,
// but does not require that the set of produced patches exactly matches the expected set of patches.
// These tests document cases where we may not produce the optimal set of patches, but still produce the correct final result.
// All of these cases are caused by the same issue: a patch generator emits unncessary leaf patches for a contiguous block of modified rows.
// TODO: Fix this issue and move these into TestPatchBasedMerging
func TestPatchBasedMergingSkipCheckingPatches(t *testing.T) {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	fourAndAHalfChunks, desc := makeSimpleIntMap(t, 1, 1250)
	chunkBoundaries := []uint32{414, 718, 830, 1207}

	emptyMap, err := tree.MakeTreeForTest(nil)
	require.NoError(t, err)

	mapWithUpdates := func(root tree.Node, updates ...mutation[uint32]) tree.Node {
		return mutate(t, ctx, root, desc, desc, updates)
	}

	t.Run("overlapping inserts to nearly empty map: inserts overlap with base", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 800, 800)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		upperMap, _ := makeSimpleIntMap(t, 751, 1847)
		upperMapChunkBoundary := []uint32{1012, 1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			for i := uint32(1001); i <= upperMapChunkBoundary[0]; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[0],
				endKey:       upperMapChunkBoundary[1],
				subtreeCount: uint64(upperMapChunkBoundary[1] - upperMapChunkBoundary[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[1],
				endKey:       upperMapChunkBoundary[2],
				subtreeCount: uint64(upperMapChunkBoundary[2] - upperMapChunkBoundary[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[2],
				endKey:       upperMapChunkBoundary[3],
				subtreeCount: uint64(upperMapChunkBoundary[3] - upperMapChunkBoundary[2]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})

			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1, chunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[0],
				endKey:       chunkBoundaries[1],
				subtreeCount: uint64(chunkBoundaries[1] - chunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, manyPointInserts(chunkBoundaries[1]+1, 750)...)

			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("overlapping inserts to nearly empty map: inserts are before base", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 2000, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		upperMap, _ := makeSimpleIntMap(t, 751, 1847)
		upperMapChunkBoundary := []uint32{1012, 1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			for i := uint32(1001); i <= upperMapChunkBoundary[0]; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[0],
				endKey:       upperMapChunkBoundary[1],
				subtreeCount: uint64(upperMapChunkBoundary[1] - upperMapChunkBoundary[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[1],
				endKey:       upperMapChunkBoundary[2],
				subtreeCount: uint64(upperMapChunkBoundary[2] - upperMapChunkBoundary[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[2],
				endKey:       upperMapChunkBoundary[3],
				subtreeCount: uint64(upperMapChunkBoundary[3] - upperMapChunkBoundary[2]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})

			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.

			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1, chunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[0],
				endKey:       chunkBoundaries[1],
				subtreeCount: uint64(chunkBoundaries[1] - chunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, manyPointInserts(chunkBoundaries[1]+1, 750)...)

			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("overlapping inserts to nearly empty map: inserts are after base", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 0, 0)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		upperMap, _ := makeSimpleIntMap(t, 751, 1847)
		upperMapChunkBoundary := []uint32{1012, 1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			for i := uint32(1001); i <= upperMapChunkBoundary[0]; i++ {
				expectedPatches = append(expectedPatches, expectedPatch[uint32]{
					level:   0,
					endKey:  i,
					toValue: i,
				})
			}
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[0],
				endKey:       upperMapChunkBoundary[1],
				subtreeCount: uint64(upperMapChunkBoundary[1] - upperMapChunkBoundary[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[1],
				endKey:       upperMapChunkBoundary[2],
				subtreeCount: uint64(upperMapChunkBoundary[2] - upperMapChunkBoundary[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundary[2],
				endKey:       upperMapChunkBoundary[3],
				subtreeCount: uint64(upperMapChunkBoundary[3] - upperMapChunkBoundary[2]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})

			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1, chunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     chunkBoundaries[0],
				endKey:       chunkBoundaries[1],
				subtreeCount: uint64(chunkBoundaries[1] - chunkBoundaries[0]),
			})
			expectedPatches = append(expectedPatches, manyPointInserts(chunkBoundaries[1]+1, 750)...)

			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("non-overlapping removals produce empty map", func(t *testing.T) {
		// The patch generator for the right branch needs to recurse into the leaves in order to know the minimum
		// key value of the first child node. A result, we get a separate patch for each removed key. This can likely
		// be improved in the future.
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		baseMapChunkBoundaries := getChunkBoundaries(t, desc, baseMap)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		upperMap, _ := makeSimpleIntMap(t, 1001, 2000)
		t.Run("right removes lower bound", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1, baseMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[0],
				endKey:    baseMapChunkBoundaries[1],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[1],
				endKey:    baseMapChunkBoundaries[2],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[2],
				endKey:    1000,
				isRemoval: true,
			})
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right removes upper bound", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1001, baseMapChunkBoundaries[3])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[3],
				endKey:    baseMapChunkBoundaries[4],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[4],
				endKey:    baseMapChunkBoundaries[5],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[5],
				endKey:    2000,
				isRemoval: true,
			})
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("non-overlapping removals produce nearly-empty map", func(t *testing.T) {
		// The patch generator for the right branch needs to recurse into the leaves in order to know the minimum
		// key value of the first child node. A result, we get a separate patch for each removed key. This can likely
		// be improved in the future.
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		baseMapChunkBoundaries := getChunkBoundaries(t, desc, baseMap)
		lowerMap, _ := makeSimpleIntMap(t, 1, 999)
		upperMap, _ := makeSimpleIntMap(t, 1001, 2000)
		t.Run("right removes lower bound", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1, baseMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[0],
				endKey:    baseMapChunkBoundaries[1],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[1],
				endKey:    baseMapChunkBoundaries[2],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[2],
				endKey:    1000,
				isRemoval: true,
			})
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right removes upper bound", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1001, baseMapChunkBoundaries[3])...)

			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[3],
				endKey:    baseMapChunkBoundaries[4],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[4],
				endKey:    baseMapChunkBoundaries[5],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[5],
				endKey:    2000,
				isRemoval: true,
			})
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("overlapping removals produce empty map", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 500)
		upperMap, _ := makeSimpleIntMap(t, 1500, 2000)
		baseMapChunkBoundaries := getChunkBoundaries(t, desc, baseMap)
		t.Run("right removes lower bound", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1, baseMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[0],
				endKey:    500,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right removes upper bound", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1500, baseMapChunkBoundaries[len(baseMapChunkBoundaries)-2])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[len(baseMapChunkBoundaries)-2],
				endKey:    2000,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("overlapping removals produce nearly empty map", func(t *testing.T) {
		// Both branches delete most of the rows in the map. After merging, only the first and last rows remain.
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		baseMapChunkBoundaries := getChunkBoundaries(t, desc, baseMap)
		lowerMap, _ := makeSimpleIntMap(t, 1, 500)
		lowerMap = mapWithUpdates(lowerMap, update[uint32](2000, 2000))
		upperMap, _ := makeSimpleIntMap(t, 1500, 2000)
		upperMap = mapWithUpdates(upperMap, update[uint32](1, 1))
		// upperMapChunkBoundary := getChunkBoundaries(t, desc, upperMap) //[]uint32{1207, 1450, 1846}
		t.Run("right removes lower bound", func(t *testing.T) {

			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1, baseMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[0],
				endKey:    500,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right removes upper bound", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1500, baseMapChunkBoundaries[len(baseMapChunkBoundaries)-2])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[len(baseMapChunkBoundaries)-2],
				endKey:    2000,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
	})

	t.Run("unequal sized inserts to nearly empty map: concurrent, right diff is superset", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 2000, 2000)
		leftMap, _ := makeSimpleIntMap(t, 500, 500)
		rightMap, _ := makeSimpleIntMap(t, 1, 831)
		rightMapChunkBoundaries := []uint32{414, 718, 830}
		var expectedPatches []expectedPatch[uint32]
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			level:        1,
			noStartKey:   true,
			endKey:       rightMapChunkBoundaries[0],
			subtreeCount: uint64(rightMapChunkBoundaries[0]),
		})
		for i := rightMapChunkBoundaries[0] + 1; i < 500; i++ {
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  i,
				toValue: i,
			})
		}
		for i := uint32(501); i < rightMapChunkBoundaries[1]; i++ {
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  i,
				toValue: i,
			})
		}
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			level:        1,
			noStartKey:   true,
			endKey:       rightMapChunkBoundaries[2],
			subtreeCount: uint64(rightMapChunkBoundaries[2] - rightMapChunkBoundaries[1]),
		})

		// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
		// This could be improved in the future.
		testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, leftMap, rightMap, expectedPatches)
	})

	t.Run("unequal sized removals produce empty map: small removal greater than than large removal", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1999)
		upperMap, _ := makeSimpleIntMap(t, 2000, 2000)
		baseMapChunkBoundaries := getChunkBoundaries(t, desc, baseMap)
		t.Run("right side removes single row", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{pointRemove[uint32](2000)}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
		t.Run("right side removes many rows", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1, baseMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[0],
				endKey:    baseMapChunkBoundaries[1],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[1],
				endKey:    baseMapChunkBoundaries[2],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[2],
				endKey:    baseMapChunkBoundaries[3],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[3],
				endKey:    baseMapChunkBoundaries[4],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[4],
				endKey:    baseMapChunkBoundaries[5],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[5],
				endKey:    1999,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
	})

	t.Run("unequal sized removals produce nearly empty map: small removal greater than than large removal", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1998)
		upperMap, _ := makeSimpleIntMap(t, 2000, 2000)
		baseMapChunkBoundaries := getChunkBoundaries(t, desc, baseMap)
		t.Run("right side removes single row", func(t *testing.T) {
			expectedPatches := []expectedPatch[uint32]{pointRemove[uint32](2000)}
			testPatchBasedMerging(t, ctx, ns, desc, nil, baseMap, upperMap, lowerMap, expectedPatches)
		})
		t.Run("right side removes many rows", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointRemoves(1, baseMapChunkBoundaries[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[0],
				endKey:    baseMapChunkBoundaries[1],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[1],
				endKey:    baseMapChunkBoundaries[2],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[2],
				endKey:    baseMapChunkBoundaries[3],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[3],
				endKey:    baseMapChunkBoundaries[4],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[4],
				endKey:    baseMapChunkBoundaries[5],
				isRemoval: true,
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:     1,
				startKey:  baseMapChunkBoundaries[5],
				endKey:    1999,
				isRemoval: true,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
	})

	t.Run("unequal sized removals produce almost empty map: concurrent, right is nearly empty", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		leftMap := mapWithUpdates(baseMap, remove[uint32](500))
		rightMap, _ := makeSimpleIntMap(t, 1000, 1000)
		rightMapChunkBoundaries := []uint32{414, 718, 830}
		var expectedPatches []expectedPatch[uint32]
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			level:        1,
			noStartKey:   true,
			endKey:       rightMapChunkBoundaries[0],
			subtreeCount: uint64(rightMapChunkBoundaries[0]),
		})
		for i := uint32(415); i < 500; i++ {
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  i,
				toValue: i,
			})
		}
		for i := uint32(501); i <= rightMapChunkBoundaries[1]; i++ {
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  i,
				toValue: i,
			})
		}
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			level:        1,
			startKey:     rightMapChunkBoundaries[1],
			endKey:       rightMapChunkBoundaries[2],
			subtreeCount: uint64(rightMapChunkBoundaries[2] - rightMapChunkBoundaries[1]),
		})

		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			level:        1,
			startKey:     rightMapChunkBoundaries[2],
			endKey:       1000,
			subtreeCount: uint64(1000 - rightMapChunkBoundaries[2]),
		})
		// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
		// This could be improved in the future.
		testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, leftMap, rightMap, expectedPatches)
	})

	t.Run("unequal sized removals produce almost empty map: concurrent, left is nearly empty", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 1, 2000)
		leftMap, _ := makeSimpleIntMap(t, 1000, 1000)
		rightMap := mapWithUpdates(baseMap, remove[uint32](500))
		// In this case, the left contains a superset of the changes from the right.
		// The merge is equal to leftMap, with no patches necessary.
		expectedPatches := []expectedPatch[uint32]{}
		testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, leftMap, rightMap, expectedPatches)
	})

	t.Run("many leaf row removals in the middle doesn't emit unnecessary leaf removals", func(t *testing.T) {
		// If we need to emit leaf removals for a continuous range of removals, we should be able to eventually emit
		// a node modification.
		var expectedPatches []expectedPatch[uint32]
		expectedPatches = append(expectedPatches, manyPointRemoves(715, 718)...)
		expectedPatches = append(expectedPatches, expectedPatch[uint32]{
			level:        1,
			startKey:     chunkBoundaries[1],
			endKey:       chunkBoundaries[3],
			subtreeCount: uint64(chunkBoundaries[3] - chunkBoundaries[2]),
		})
		expectedPatches = append(expectedPatches, manyPointRemoves(1207, 1210)...)
		testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil,
			fourAndAHalfChunks,
			mapWithUpdates(fourAndAHalfChunks, remove[uint32](710)),
			mapWithUpdates(fourAndAHalfChunks, makeDeletePatches(715, 1210)...),
			expectedPatches)
	})

	t.Run("unequal sized inserts to nearly empty map: small insert, then large insert, then base", func(t *testing.T) {
		baseMap, _ := makeSimpleIntMap(t, 2000, 2000)
		lowerMap, _ := makeSimpleIntMap(t, 1, 1)
		upperMap, _ := makeSimpleIntMap(t, 1001, 1847)
		upperMapChunkBoundaires := []uint32{1237, 1450, 1846}
		t.Run("right map is the upper map", func(t *testing.T) {
			var expectedPatches []expectedPatch[uint32]
			expectedPatches = append(expectedPatches, manyPointInserts(1001, upperMapChunkBoundaires[0])...)
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundaires[0],
				endKey:       upperMapChunkBoundaires[1],
				subtreeCount: uint64(upperMapChunkBoundaires[1] - upperMapChunkBoundaires[0]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:        1,
				startKey:     upperMapChunkBoundaires[1],
				endKey:       upperMapChunkBoundaires[2],
				subtreeCount: uint64(upperMapChunkBoundaires[2] - upperMapChunkBoundaires[1]),
			})
			expectedPatches = append(expectedPatches, expectedPatch[uint32]{
				level:   0,
				endKey:  1847,
				toValue: 1847,
			})
			// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
			// This could be improved in the future.
			testPatchBasedMergingResultsOnly(t, ctx, ns, desc, nil, baseMap, lowerMap, upperMap, expectedPatches)
		})
		t.Run("right map is the lower map", func(t *testing.T) {
			var expectedPatches = []expectedPatch[uint32]{
				pointUpdate[uint32](1, 1),
			}
			testPatchBasedMerging(t, ctx, ns, desc, nil, emptyMap, upperMap, lowerMap, expectedPatches)
		})
	})
}
