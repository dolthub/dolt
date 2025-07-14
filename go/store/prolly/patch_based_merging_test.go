package prolly

import (
	"context"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"golang.org/x/sync/errgroup"
)

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

func testPatchBasedMerging[T interface{}](
	t *testing.T,
	ctx context.Context,
	ns tree.NodeStore,
	keyDesc val.TupleDesc,
	baseRoot, leftRoot, rightRoot tree.Node,
	expectedPatches []expectedPatch[T],
	collide tree.CollisionFn,
) {
	err := producePatches(ctx, ns, leftRoot, rightRoot, baseRoot, collide, keyDesc, func(iter tree.PatchIter) error {
		var actualPatches []tree.Patch
		actualPatch, err := iter.NextPatch(ctx)
		require.NoError(t, err)
		for actualPatch.EndKey != nil {
			actualPatches = append(actualPatches, actualPatch)
			actualPatch, err = iter.NextPatch(ctx)
			require.NoError(t, err)
		}
		require.Equal(t, len(expectedPatches), len(actualPatches), "expected %d patches but found %d", len(expectedPatches), len(actualPatches))
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
				assert.True(t, expectedPatch.isRemoval)
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
		traditionalMergeRoot, _, err := tree.ThreeWayMerge(ctx, ns, leftRoot, rightRoot, baseRoot, collide, keyDesc, serializer)
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
	threeAndAHalfChunks, desc := makeSimpleIntMap(t, 1, 1024)
	threeChunks, _ := makeSimpleIntMap(t, 1, 830)
	// The base map will happen to have these chunk boundaries.
	chunkBoundaries := []uint32{414, 718, 830}
	maxKey := uint32(1024)

	mapWithUpdates := func(root tree.Node, updates ...mutation[uint32]) tree.Node {
		return mutate(t, ctx, root, desc, desc, updates)
	}

	stringMapWithUpdates := func(root tree.Node, updates ...mutation[string]) tree.Node {
		return mutateStrings(t, ctx, root, desc, desc, updates)
	}

	t.Run("concurrent updates in same leaf node produce level 0 patch", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 0)), mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[0]-4, 1)), []expectedPatch[uint32]{pointUpdate(chunkBoundaries[0]-4, 1)}, nil)
	})

	t.Run("update in first child node produces top-level patch with no start key", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](1000, 1)), mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[0]-10, 1)), []expectedPatch[uint32]{
			{
				noStartKey:   true,
				endKey:       chunkBoundaries[0],
				level:        1,
				subtreeCount: uint64(chunkBoundaries[0]),
			},
		}, nil)
	})

	t.Run("update in middle child node produces top-level patch with start key", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 1)), mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[2]-50, 1)), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[1],
				endKey:       chunkBoundaries[2],
				level:        1,
				subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
			},
		}, nil)
	})

	t.Run("update in final child node produces top-level patch with start key", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 1)), mapWithUpdates(threeAndAHalfChunks, update[uint32](maxKey-100, 1)), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[2],
				endKey:       maxKey,
				level:        1,
				subtreeCount: uint64(maxKey - chunkBoundaries[2]),
			},
		}, nil)
	})

	t.Run("an insert beyond the final key doesn't create an extra chunk boundary", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](0, 1)), mapWithUpdates(threeAndAHalfChunks, update[uint32](maxKey+1, 1)), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[2],
				endKey:       maxKey + 1,
				level:        1,
				subtreeCount: uint64(maxKey + 1 - chunkBoundaries[2]),
			},
		}, nil)
	})

	t.Run("an insert before the first key doesn't create an extra chunk boundary", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, update[uint32](maxKey+1, 1)), mapWithUpdates(threeAndAHalfChunks, update[uint32](0, 1)), []expectedPatch[uint32]{
			{
				noStartKey:   true,
				endKey:       chunkBoundaries[0],
				level:        1,
				subtreeCount: uint64(chunkBoundaries[0] + 1),
			},
		}, nil)
	})

	t.Run("removing a value from a leaf node produces a top-level patch", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)), mapWithUpdates(threeAndAHalfChunks, remove[uint32](maxKey-20)), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[2],
				endKey:       maxKey,
				level:        1,
				subtreeCount: uint64(maxKey - chunkBoundaries[2] - 1),
			},
		}, nil)
	})

	t.Run("concurrent removals in a leaf node produces a leaf-level patch", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)), mapWithUpdates(threeAndAHalfChunks, remove[uint32](2)), []expectedPatch[uint32]{
			{
				startKey:     1,
				endKey:       2,
				level:        0,
				subtreeCount: 1,
				isRemoval:    true,
			},
		}, nil)
	})

	t.Run("deleting an entire chunk at the end produces a single range", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)), mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(chunkBoundaries[2]+1, maxKey)...), []expectedPatch[uint32]{
			{
				startKey:  chunkBoundaries[2],
				endKey:    maxKey,
				level:     1,
				isRemoval: true,
			},
		}, nil)
	})

	// We expect removing an entire chunk produces a single range covering the removed chunk and the subsequent one.
	t.Run("deleting an entire chunk in the middle", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)), mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(chunkBoundaries[1]+1, chunkBoundaries[2])...), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[1],
				endKey:       maxKey,
				level:        1,
				subtreeCount: uint64(maxKey - chunkBoundaries[2]),
			},
		}, nil)
	})

	t.Run("deleting an entire chunk concurrent with a point delete", func(t *testing.T) {
		// This needs to recurse into the leaf node in order to verify that the concurrently modified key isn't a conflict.
		// Once we confirm there's no conflict, we could potentially emit a single high-level patch, but the logic would be
		// more complicated. So currently we emit multiple patches instead.
		expectedPatches := manyPointRemoves(831, 999)
		expectedPatches = append(expectedPatches, manyPointRemoves(1001, 1024)...)
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1000)), mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(831, 1024)...), expectedPatches, nil)
	})

	t.Run("deleting many rows at the end", func(t *testing.T) {
		// We expect the chunk before the deleted rows to be modified, and then a removal patch for each chunk until the end.
		// Again, we could potentially emit a single removal patch, or even a single modified patch, but the logic would be
		// more complicated.
		newMaxKey := uint32(819)
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1)), mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(newMaxKey+1, maxKey)...), []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[1],
				endKey:       newMaxKey,
				level:        1,
				subtreeCount: uint64(newMaxKey - chunkBoundaries[1]),
			},
			{
				startKey:  newMaxKey,
				endKey:    chunkBoundaries[2],
				level:     1,
				isRemoval: true,
			},
			{
				startKey:  chunkBoundaries[2],
				endKey:    1024,
				level:     1,
				isRemoval: true,
			},
		}, nil)
	})

	t.Run("deleting many rows at the end doesn't emit unnecessary leaf removals", func(t *testing.T) {
		newMaxKey := chunkBoundaries[2] - 1
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](818)), mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(newMaxKey+1, maxKey)...), []expectedPatch[uint32]{
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
		}, nil)
	})

	t.Run("deleting an chunk boundary produces new boundaries until they realign", func(t *testing.T) {
		newChunkBoundary := uint32(436)
		testPatchBasedMerging(t, ctx, ns, desc, threeAndAHalfChunks, mapWithUpdates(threeAndAHalfChunks, remove[uint32](1000)), mapWithUpdates(threeAndAHalfChunks, remove[uint32](chunkBoundaries[0])), []expectedPatch[uint32]{
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
		}, nil)
	})

	t.Run("range insert at end", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc, threeChunks, mapWithUpdates(threeChunks, update[uint32](1, 0)), threeAndAHalfChunks, []expectedPatch[uint32]{
			{
				startKey:     chunkBoundaries[2],
				endKey:       maxKey,
				level:        1,
				subtreeCount: uint64(maxKey - chunkBoundaries[2]),
			},
		}, nil)
	})

	t.Run("non-overlapping inserts to empty map", func(t *testing.T) {
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		upperMap, _ := makeSimpleIntMap(t, 1001, 2000)
		upperMapChunkBoundary := []uint32{1237, 1450, 1846}
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
			endKey:       2000,
			subtreeCount: uint64(2000 - upperMapChunkBoundary[2]),
		})
		// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
		// This could be improved in the future.
		testPatchBasedMerging(t, ctx, ns, desc, emptyMap, lowerMap, upperMap, expectedPatches, nil)
	})

	t.Run("overlapping inserts to empty map", func(t *testing.T) {
		lowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		upperMap, _ := makeSimpleIntMap(t, 501, 2000)
		upperMapChunkBoundary := []uint32{1207, 1450, 1846}
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
			endKey:       2000,
			subtreeCount: uint64(2000 - upperMapChunkBoundary[2]),
		})
		// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
		// This could be improved in the future.
		testPatchBasedMerging(t, ctx, ns, desc, emptyMap, lowerMap, upperMap, expectedPatches, nil)
	})

	t.Run("unequal sized inserts to empty map: small before large", func(t *testing.T) {
		leftLowerMap, _ := makeSimpleIntMap(t, 1, 1)
		rightUpperMap, _ := makeSimpleIntMap(t, 1001, 2000)
		rightMapChunkBoundaries := []uint32{1237, 1450, 1846}
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
			level:        1,
			startKey:     rightMapChunkBoundaries[2],
			endKey:       2000,
			subtreeCount: uint64(2000 - rightMapChunkBoundaries[2]),
		})
		// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
		// This could be improved in the future.
		testPatchBasedMerging(t, ctx, ns, desc, emptyMap, leftLowerMap, rightUpperMap, expectedPatches, nil)
	})

	t.Run("unequal sized inserts to empty map: small after large", func(t *testing.T) {
		leftUpperMap, _ := makeSimpleIntMap(t, 2000, 2000)
		rightLowerMap, _ := makeSimpleIntMap(t, 1, 1000)
		rightMapChunkBoundaries := []uint32{414, 718, 830}
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
			level:        1,
			startKey:     rightMapChunkBoundaries[2],
			endKey:       1000,
			subtreeCount: uint64(1000 - rightMapChunkBoundaries[2]),
		})
		// Because nodes don't store their lower bound, we have to recurse to the leaf level for the first patches.
		// This could be improved in the future.
		testPatchBasedMerging(t, ctx, ns, desc, emptyMap, leftUpperMap, rightLowerMap, expectedPatches, nil)
	})

	t.Run("unequal sized inserts to empty map: concurrent", func(t *testing.T) {
		leftUpperMap, _ := makeSimpleIntMap(t, 500, 500)
		rightLowerMap, _ := makeSimpleIntMap(t, 1, 1000)
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
		testPatchBasedMerging(t, ctx, ns, desc, emptyMap, leftUpperMap, rightLowerMap, expectedPatches, nil)
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
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeChunks,
			mapWithUpdates(threeChunks, update[uint32](1, 10), update[uint32](990, 0)),
			mapWithUpdates(threeChunks, update[uint32](1, 20), update[uint32](1000, 0)),
			[]expectedPatch[uint32]{
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
			},
			collide,
		)
	})

	t.Run("merge with unresolvable conflicts", func(t *testing.T) {
		var conflicts []struct{ left, right tree.Diff }
		// A custom callback records all conflicts and treats them as unresolvable.
		collide := func(left, right tree.Diff) (tree.Diff, bool) {
			conflicts = append(conflicts, struct{ left, right tree.Diff }{left, right})
			return tree.Diff{}, false
		}
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 10), update[uint32](500, 0)),
			mapWithUpdates(threeAndAHalfChunks, update[uint32](1, 20), update[uint32](1000, 0)),
			[]expectedPatch[uint32]{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey,
					level:        1,
					subtreeCount: uint64(maxKey - chunkBoundaries[2]),
				},
			},
			collide,
		)
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
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update[uint32](chunkBoundaries[2]-30, 0)),
			mapWithUpdates(threeAndAHalfChunks, append(makeDeletePatches(chunkBoundaries[0]+1, chunkBoundaries[1]), update[uint32](chunkBoundaries[2]-30, 0))...), []expectedPatch[uint32]{
				{
					startKey:     chunkBoundaries[0],
					endKey:       chunkBoundaries[2],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
				},
			}, nil)
	})

	t.Run("multi-level tree: deleting many rows off the end produces minimal diffs", func(t *testing.T) {
		desc = val.NewTupleDescriptor(val.Type{Enc: val.StringEnc})
		bld := val.NewTupleBuilder(desc, nil)
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
		testPatchBasedMerging[string](t, ctx, ns, desc, baseRoot, leftRoot, rightRoot, []expectedPatch[string]{
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
		}, nil)
	})
}
