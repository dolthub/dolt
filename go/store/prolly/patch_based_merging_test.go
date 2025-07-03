package prolly

import (
	"context"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func (s *slicePatchIter) NextPatch(ctx context.Context) tree.Patch {
	if s.index >= len(s.slice) {
		return tree.Patch{}
	}
	result := s.slice[s.index]
	s.index++
	return result
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
	ld := tree.PatchGeneratorFromRoots[K](ctx, ns, ns, base, left, order)

	rd := tree.PatchGeneratorFromRoots[K](ctx, ns, ns, base, right, order)

	eg, ctx := errgroup.WithContext(ctx)
	patches := tree.NewPatchBuffer(tree.PatchBufferSize)

	// iterate |ld| and |rd| in parallel, populating |patches|
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

// pointPatch is a description of a single key-value change, used for tests.
type pointPatch struct {
	key   uint32
	value *uint32
}

func ptr(i uint32) *uint32 {
	return &i
}

func update(key, value uint32) pointPatch {
	return pointPatch{key, ptr(value)}
}

func remove(key uint32) pointPatch {
	return pointPatch{key, nil}
}

type rangePatch struct {
	startKey, endKey uint32
	noStartKey       bool
	level            int
	subtreeCount     uint64
	toValue          uint32
	isRemoval        bool
}

func pointUpdate(key, value uint32) rangePatch {
	return rangePatch{
		startKey:     key - 1,
		endKey:       key,
		noStartKey:   false,
		level:        0,
		subtreeCount: 1,
		toValue:      value,
		isRemoval:    false,
	}
}

func pointRemove(key uint32) rangePatch {
	return rangePatch{
		startKey:   key - 1,
		endKey:     key,
		noStartKey: false,
		isRemoval:  true,
	}
}

func manyPointRemoves(keyStart, keyEnd uint32) (patches []rangePatch) {
	for i := keyStart; i <= keyEnd; i++ {
		patches = append(patches, pointRemove(i))
	}
	return patches
}

func testPatchBasedMerging(
	t *testing.T,
	ctx context.Context,
	ns tree.NodeStore,
	keyDesc val.TupleDesc,
	baseRoot, leftRoot, rightRoot tree.Node,
	expectedPatches []rangePatch,
) {
	err := producePatches(ctx, ns, leftRoot, rightRoot, baseRoot, nil, keyDesc, func(iter tree.PatchIter) error {
		var actualPatches []tree.Patch
		actualPatch := iter.NextPatch(ctx)
		for actualPatch.EndKey != nil {
			actualPatches = append(actualPatches, actualPatch)
			actualPatch = iter.NextPatch(ctx)
		}
		require.Equal(t, len(expectedPatches), len(actualPatches), "expected %d patches but found %d", len(expectedPatches), len(actualPatches))
		for i, actualPatch := range actualPatches {
			expectedPatch := expectedPatches[i]
			require.Equal(t, expectedPatch.level, actualPatch.Level)
			if expectedPatch.noStartKey || expectedPatch.level == 0 {
				assert.Nil(t, actualPatch.KeyBelowStart)
			} else {
				require.NotNil(t, actualPatch.KeyBelowStart)
				actualStartKey, _ := keyDesc.GetUint32(0, val.Tuple(actualPatch.KeyBelowStart))
				assert.Equal(t, expectedPatch.startKey, actualStartKey, "patch %d has unexpected start key. Expected %d, found %d", i, expectedPatch.startKey, actualStartKey)
			}

			actualEndKey, _ := keyDesc.GetUint32(0, val.Tuple(actualPatch.EndKey))
			assert.Equal(t, expectedPatch.endKey, actualEndKey, "patch %d has unexpected end key. Expected %d, found %d", i, expectedPatch.endKey, actualEndKey)
			if actualPatch.To == nil {
				assert.True(t, expectedPatch.isRemoval)
			} else {
				if actualPatch.Level > 0 {
					assert.Equal(t, expectedPatch.subtreeCount, actualPatch.SubtreeCount)
					expectedAddress, ok, err := tree.GetAddressFromLevelAndKeyForTest(ctx, ns, rightRoot, actualPatch.Level, val.Tuple(actualPatch.EndKey), keyDesc)
					require.NoError(t, err)
					require.True(t, ok)
					assert.Equal(t, tree.Item(expectedAddress[:]), actualPatch.To)
				} else {
					actualToValue, _ := keyDesc.GetUint32(0, val.Tuple(actualPatch.To))
					assert.Equal(t, expectedPatch.toValue, actualToValue)
				}
			}
		}

		patchIter := slicePatchIter{slice: actualPatches}

		serializer := message.NewProllyMapSerializer(keyDesc, ns.Pool())
		// verify that this is equivalent to a traditional merge.
		traditionalMergeRoot, _, err := tree.ThreeWayMerge(ctx, ns, leftRoot, rightRoot, baseRoot, nil, keyDesc, serializer)
		mergedRoot, err := tree.ApplyPatches(ctx, ns, leftRoot, keyDesc, serializer, &patchIter)
		require.NoError(t, err)
		assert.Equal(t, mergedRoot, traditionalMergeRoot)

		return nil
	})
	require.NoError(t, err)
}

func mutate(t *testing.T, ctx context.Context, baseRoot tree.Node, keyDesc, valDesc val.TupleDesc, patches []pointPatch) tree.Node {
	baseMap := NewMap(baseRoot, ns, keyDesc, valDesc)

	keyBld := val.NewTupleBuilder(keyDesc, ns)
	valBld := val.NewTupleBuilder(valDesc, ns)
	var err error

	mutMap := baseMap.Mutate()
	for _, m := range patches {
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

func makeDeletePatches(start, stop uint32) (patches []pointPatch) {
	for i := start; i <= stop; i++ {
		patches = append(patches, pointPatch{key: i, value: nil})
	}
	return patches
}

func makeSimpleIntMap(t *testing.T, start, stop int) (tree.Node, val.TupleDesc) {
	tups, desc := tree.AscendingUintTuplesWithStep(stop-start+1, start, start, 1)
	root, err := tree.MakeTreeForTest(tups)
	require.NoError(t, err)
	return root, desc
}

// TestPatchBasedMerging tests that The patching process should produce the minimal set of patches which must be applied to the left tree from the right tree.
func TestPatchBasedMerging(t *testing.T) {

	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	threeAndAHalfChunks, desc := makeSimpleIntMap(t, 1, 1024)
	threeChunks, _ := makeSimpleIntMap(t, 1, 830)
	// twoAndAHalfChunks, _ := makeSimpleIntMap(t, 1, 800)
	// The base map will happen to have these chunk boundaries.
	chunkBoundaries := []uint32{414, 718, 830}
	maxKey := uint32(1024)

	mapWithUpdates := func(root tree.Node, updates ...pointPatch) tree.Node {
		return mutate(t, ctx, root, desc, desc, updates)
	}

	t.Run("concurrent updates in same leaf node produce level 0 patch", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc, threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1, 0)),
			mapWithUpdates(threeAndAHalfChunks, update(chunkBoundaries[0]-4, 1)),
			[]rangePatch{pointUpdate(chunkBoundaries[0]-4, 1)},
		)
	})

	t.Run("update in first child node produces top-level patch with no start key", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1000, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(chunkBoundaries[0]-10, 1)),
			[]rangePatch{
				{
					noStartKey:   true,
					endKey:       chunkBoundaries[0],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[0]),
				},
			},
		)
	})

	t.Run("update in middle child node produces top-level patch with start key", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(chunkBoundaries[2]-50, 1)),
			[]rangePatch{
				{
					startKey:     chunkBoundaries[1],
					endKey:       chunkBoundaries[2],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
				},
			})
	})

	t.Run("update in final child node produces top-level patch with start key", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(maxKey-100, 1)),
			[]rangePatch{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey,
					level:        1,
					subtreeCount: uint64(maxKey - chunkBoundaries[2]),
				},
			})
	})

	t.Run("an insert beyond the final key doesn't create an extra chunk boundary", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(0, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(maxKey+1, 1)),
			[]rangePatch{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey + 1,
					level:        1,
					subtreeCount: uint64(maxKey + 1 - chunkBoundaries[2]),
				},
			})
	})

	t.Run("an insert before the first key doesn't create an extra chunk boundary", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(maxKey+1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(0, 1)),
			[]rangePatch{
				{
					noStartKey:   true,
					endKey:       chunkBoundaries[0],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[0] + 1),
				},
			})
	})

	// Tests that insert and remove
	t.Run("removing a value from a leaf node produces a top-level patch", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, remove(maxKey-20)),
			[]rangePatch{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey,
					level:        1,
					subtreeCount: uint64(maxKey - chunkBoundaries[2] - 1),
				},
			})
	})

	t.Run("concurrent removals in a leaf node produces a leaf-level patch", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, remove(2)),
			[]rangePatch{
				{
					startKey:     1,
					endKey:       2,
					level:        0,
					subtreeCount: 1,
					isRemoval:    true,
				},
			})
	})

	t.Run("deleting an entire chunk produces a single range", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(831, 1024)...),
			[]rangePatch{
				{
					startKey:  chunkBoundaries[2],
					endKey:    maxKey,
					level:     1,
					isRemoval: true,
				},
			})
	})

	// This needs to recurse into the leaf node in order to verify that the concurrently modified isn't a conflict.
	// Once we confirm there's no conflict, we could potentially emit a single range diff, but the logic would be
	// more complicated.
	expectedPatches := manyPointRemoves(831, 999)
	expectedPatches = append(expectedPatches, manyPointRemoves(1001, 1024)...)
	t.Run("deleting an entire chunk concurrent with a point delete", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1000)),
			mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(831, 1024)...),
			expectedPatches,
		)
	})

	t.Run("deleting an entire chunk and some previous rows", func(t *testing.T) {
		testPatchBasedMerging(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, makeDeletePatches(820, 1024)...),
			[]rangePatch{
				{
					startKey:     chunkBoundaries[1],
					endKey:       819,
					level:        1,
					subtreeCount: uint64(819 - chunkBoundaries[1]),
				},
				{
					startKey:  819,
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
			})
	})

	t.Run("range insert at end", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc,
			threeChunks,
			mapWithUpdates(threeChunks, update(1, 0)),
			threeAndAHalfChunks,
			[]rangePatch{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey,
					level:        1,
					subtreeCount: uint64(maxKey - chunkBoundaries[2]),
				},
			},
		)
	})

	t.Run("identical changes to overlapping regions", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc,
			threeChunks,
			mapWithUpdates(threeChunks, update(1, 0), update(2, 0)),
			mapWithUpdates(threeChunks, update(2, 0), update(3, 0)),
			[]rangePatch{
				pointUpdate(3, 0),
			},
		)
	})

	// Tests that change the chunk boundaries
	newChunkBoundary := uint32(436)
	t.Run("deleting a chunk boundary shifts the chunk boundaries and produces range diffs until they re-align", func(t *testing.T) {
		testPatchBasedMerging(t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1000)),
			mapWithUpdates(threeAndAHalfChunks, remove(414)),
			[]rangePatch{
				{
					noStartKey:   true,
					endKey:       newChunkBoundary,
					level:        1,
					subtreeCount: uint64(newChunkBoundary) - 1,
				},
				{
					startKey:     newChunkBoundary,
					endKey:       chunkBoundaries[1],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[1] - newChunkBoundary),
				},
			})
	})
}
