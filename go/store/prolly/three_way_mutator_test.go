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

// sliceMutationIter implements tree.MutationIter and is used in tests to inspect a list of mutations from a RangeDiffer
// before applying them.
type sliceMutationIter struct {
	slice []tree.Mutation
	index int
}

var _ tree.MutationIter = &sliceMutationIter{}

func (s *sliceMutationIter) NextMutation(ctx context.Context) tree.Mutation {
	if s.index >= len(s.slice) {
		return tree.Mutation{}
	}
	result := s.slice[s.index]
	s.index++
	return result
}

func (s *sliceMutationIter) Close() error {
	return nil
}

// produceMutations produces a set of mutations that can be applied to the left tree to produce the result of a three way merge.
// It passes the produced MutationIter to the callback function.
func produceMutations[K ~[]byte, O tree.Ordering[K]](
	ctx context.Context,
	ns tree.NodeStore,
	left, right, base tree.Node,
	collide tree.CollisionFn,
	leftSchemaChange, rightSchemaChange bool,
	order O,
	cb func(buffer tree.MutationIter) error,
) (stats tree.MergeStats, err error) {
	ld, err := tree.RangeDifferFromRoots[K](ctx, ns, ns, base, left, order, leftSchemaChange)
	if err != nil {
		return tree.MergeStats{}, err
	}

	rd, err := tree.RangeDifferFromRoots[K](ctx, ns, ns, base, right, order, rightSchemaChange)
	if err != nil {
		return tree.MergeStats{}, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	patches := tree.NewPatchBuffer(tree.PatchBufferSize)

	// iterate |ld| and |rd| in parallel, populating |patches|
	eg.Go(func() (err error) {
		defer func() {
			if cerr := patches.Close(); err == nil {
				err = cerr
			}
		}()
		stats, err = tree.SendPatches(ctx, ld, rd, patches, collide)
		return
	})

	eg.Go(func() error {
		return cb(patches)
	})

	if err = eg.Wait(); err != nil {
		return tree.MergeStats{}, err
	}

	return stats, nil
}

// pointMutation is a description of a single key-value change, used for tests.
type pointMutation struct {
	key   uint32
	value *uint32
}

func ptr(i uint32) *uint32 {
	return &i
}

func update(key, value uint32) pointMutation {
	return pointMutation{key, ptr(value)}
}

func remove(key uint32) pointMutation {
	return pointMutation{key, nil}
}

type rangeMutation struct {
	startKey, endKey uint32
	noStartKey       bool
	level            int
	subtreeCount     uint64
	toValue          uint32
	isRemoval        bool
}

func pointUpdate(key, value uint32) rangeMutation {
	return rangeMutation{
		startKey:     key - 1,
		endKey:       key,
		noStartKey:   false,
		level:        0,
		subtreeCount: 1,
		toValue:      value,
		isRemoval:    false,
	}
}

func pointRemove(key uint32) rangeMutation {
	return rangeMutation{
		startKey:   key - 1,
		endKey:     key,
		noStartKey: false,
		isRemoval:  true,
	}
}

func manyPointRemoves(keyStart, keyEnd uint32) (mutations []rangeMutation) {
	for i := keyStart; i <= keyEnd; i++ {
		mutations = append(mutations, pointRemove(i))
	}
	return mutations
}

func testThreeWayMutator(
	t *testing.T,
	ctx context.Context,
	ns tree.NodeStore,
	keyDesc val.TupleDesc,
	baseRoot, leftRoot, rightRoot tree.Node,
	expectedMutations []rangeMutation,
) {
	_, err := produceMutations(ctx, ns, leftRoot, rightRoot, baseRoot, nil, false, false, keyDesc, func(iter tree.MutationIter) error {
		var actualMutations []tree.Mutation
		actualMutation := iter.NextMutation(ctx)
		for actualMutation.Key != nil {
			actualMutations = append(actualMutations, actualMutation)
			actualMutation = iter.NextMutation(ctx)
		}
		require.Equal(t, len(expectedMutations), len(actualMutations), "expected %d mutations but found %d", len(expectedMutations), len(actualMutations))
		for i, actualMutation := range actualMutations {
			expectedMutation := expectedMutations[i]

			if expectedMutation.noStartKey {
				assert.Nil(t, actualMutation.PreviousKey)
			} else {
				require.NotNil(t, actualMutation.PreviousKey)
				actualStartKey, _ := keyDesc.GetUint32(0, val.Tuple(actualMutation.PreviousKey))
				assert.Equal(t, expectedMutation.startKey, actualStartKey, "mutation %d has unexpected start key. Expected %d, found %d", i, expectedMutation.startKey, actualStartKey)
			}

			actualEndKey, _ := keyDesc.GetUint32(0, val.Tuple(actualMutation.Key))
			assert.Equal(t, expectedMutation.endKey, actualEndKey, "mutation %d has unexpected end key. Expected %d, found %d", i, expectedMutation.endKey, actualEndKey)
			if actualMutation.To == nil {
				assert.True(t, expectedMutation.isRemoval)
			} else {
				assert.Equal(t, expectedMutation.level, actualMutation.Level)
				assert.Equal(t, expectedMutation.subtreeCount, actualMutation.SubtreeCount)

				if actualMutation.Level > 0 {
					expectedAddress, ok, err := tree.GetAddressFromLevelAndKeyForTest(ctx, ns, rightRoot, actualMutation.Level, val.Tuple(actualMutation.Key), keyDesc)
					require.NoError(t, err)
					require.True(t, ok)
					assert.Equal(t, tree.Item(expectedAddress[:]), actualMutation.To)
				} else {
					actualToValue, _ := keyDesc.GetUint32(0, val.Tuple(actualMutation.To))
					assert.Equal(t, expectedMutation.toValue, actualToValue)
				}
			}
		}

		mutationIter := sliceMutationIter{slice: actualMutations}

		serializer := message.NewProllyMapSerializer(keyDesc, ns.Pool())
		// verify that this is equivalent to a traditional merge.
		traditionalMergeRoot, _, err := tree.ThreeWayMerge(ctx, ns, leftRoot, rightRoot, baseRoot, nil, false, false, keyDesc, serializer)
		mergedRoot, err := tree.ApplyMutations(ctx, ns, leftRoot, keyDesc, serializer, &mutationIter)
		require.NoError(t, err)
		assert.Equal(t, mergedRoot, traditionalMergeRoot)

		return nil
	})
	require.NoError(t, err)
}

func mutate(t *testing.T, ctx context.Context, baseRoot tree.Node, keyDesc, valDesc val.TupleDesc, mutations []pointMutation) tree.Node {
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

func makeDeleteMutations(start, stop uint32) (mutations []pointMutation) {
	for i := start; i <= stop; i++ {
		mutations = append(mutations, pointMutation{key: i, value: nil})
	}
	return mutations
}

func makeSimpleIntMap(t *testing.T, start, stop int) (tree.Node, val.TupleDesc) {
	tups, desc := tree.AscendingUintTuplesWithStep(stop-start+1, start, start, 1)
	root, err := tree.MakeTreeForTest(tups)
	require.NoError(t, err)
	return root, desc
}

// The ThreeWayMerge process should produce the minimal set of mutations which must be applied to the left tree from the right tree.
func TestThreeWayMutator(t *testing.T) {

	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	threeAndAHalfChunks, desc := makeSimpleIntMap(t, 1, 1024)
	threeChunks, _ := makeSimpleIntMap(t, 1, 830)
	// twoAndAHalfChunks, _ := makeSimpleIntMap(t, 1, 800)
	// The base map will happen to have these chunk boundaries.
	chunkBoundaries := []uint32{414, 718, 830}
	maxKey := uint32(1024)

	mapWithUpdates := func(root tree.Node, updates ...pointMutation) tree.Node {
		return mutate(t, ctx, root, desc, desc, updates)
	}

	t.Run("concurrent updates in same leaf node produce level 0 mutation", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc, threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1, 0)),
			mapWithUpdates(threeAndAHalfChunks, update(chunkBoundaries[0]-4, 1)),
			[]rangeMutation{pointUpdate(chunkBoundaries[0]-4, 1)},
		)
	})

	t.Run("update in first child node produces top-level mutation with no start key", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1000, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(chunkBoundaries[0]-10, 1)),
			[]rangeMutation{
				{
					noStartKey:   true,
					endKey:       chunkBoundaries[0],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[0]),
				},
			},
		)
	})

	t.Run("update in middle child node produces top-level mutation with start key", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(chunkBoundaries[2]-50, 1)),
			[]rangeMutation{
				{
					startKey:     chunkBoundaries[1],
					endKey:       chunkBoundaries[2],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[2] - chunkBoundaries[1]),
				},
			})
	})

	t.Run("update in final child node produces top-level mutation with start key", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(maxKey-100, 1)),
			[]rangeMutation{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey,
					level:        1,
					subtreeCount: uint64(maxKey - chunkBoundaries[2]),
				},
			})
	})

	t.Run("an insert beyond the final key doesn't create an extra chunk boundary", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(0, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(maxKey+1, 1)),
			[]rangeMutation{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey + 1,
					level:        1,
					subtreeCount: uint64(maxKey + 1 - chunkBoundaries[2]),
				},
			})
	})

	t.Run("an insert before the first key doesn't create an extra chunk boundary", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, update(maxKey+1, 1)),
			mapWithUpdates(threeAndAHalfChunks, update(0, 1)),
			[]rangeMutation{
				{
					noStartKey:   true,
					endKey:       chunkBoundaries[0],
					level:        1,
					subtreeCount: uint64(chunkBoundaries[0] + 1),
				},
			})
	})

	// Tests that insert and remove
	t.Run("removing a value from a leaf node produces a top-level mutation", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, remove(maxKey-20)),
			[]rangeMutation{
				{
					startKey:     chunkBoundaries[2],
					endKey:       maxKey,
					level:        1,
					subtreeCount: uint64(maxKey - chunkBoundaries[2] - 1),
				},
			})
	})

	t.Run("concurrent removals in a leaf node produces a leaf-level mutation", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, remove(2)),
			[]rangeMutation{
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
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, makeDeleteMutations(831, 1024)...),
			[]rangeMutation{
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
	expectedMutations := manyPointRemoves(831, 999)
	expectedMutations = append(expectedMutations, manyPointRemoves(1001, 1024)...)
	t.Run("deleting a an entire chunk concurrent with a point delete", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1000)),
			mapWithUpdates(threeAndAHalfChunks, makeDeleteMutations(831, 1024)...),
			expectedMutations,
		)
	})

	t.Run("deleting a an entire chunk and some previous rows", func(t *testing.T) {
		testThreeWayMutator(
			t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1)),
			mapWithUpdates(threeAndAHalfChunks, makeDeleteMutations(820, 1024)...),
			[]rangeMutation{
				{
					startKey:     chunkBoundaries[1],
					endKey:       819,
					level:        1,
					subtreeCount: uint64(819 - chunkBoundaries[1]),
				},
				{
					startKey:  819,
					endKey:    chunkBoundaries[2],
					isRemoval: true,
				},
				{
					startKey:  chunkBoundaries[2],
					endKey:    1024,
					isRemoval: true,
				},
			})
	})

	t.Run("range insert at end", func(t *testing.T) {
		testThreeWayMutator(t, ctx, ns, desc,
			threeChunks,
			mapWithUpdates(threeChunks, update(1, 0)),
			threeAndAHalfChunks,
			[]rangeMutation{
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
		testThreeWayMutator(t, ctx, ns, desc,
			threeChunks,
			mapWithUpdates(threeChunks, update(1, 0), update(2, 0)),
			mapWithUpdates(threeChunks, update(2, 0), update(3, 0)),
			[]rangeMutation{
				pointUpdate(3, 0),
			},
		)
	})

	// Tests that change the chunk boundaries
	newChunkBoundary := uint32(436)
	t.Run("deleting a chunk boundary shifts the chunk boundaries and produces range diffs until they re-align", func(t *testing.T) {
		testThreeWayMutator(t, ctx, ns, desc,
			threeAndAHalfChunks,
			mapWithUpdates(threeAndAHalfChunks, remove(1000)),
			mapWithUpdates(threeAndAHalfChunks, remove(414)),
			[]rangeMutation{
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
