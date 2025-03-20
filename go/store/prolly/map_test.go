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
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var testRand = rand.New(rand.NewSource(1))
var sharedPool = pool.NewBuffPool()

func TestMap(t *testing.T) {
	ctx := context.Background()
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test prolly map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			prollyMap, tuples := makeProllyMap(t, s)

			t.Run("get item from map", func(t *testing.T) {
				testGet(t, prollyMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testIterAll(t, prollyMap, tuples)
			})
			t.Run("iter range", func(t *testing.T) {
				testIterRange(t, prollyMap, tuples)
			})
			t.Run("iter ordinal range", func(t *testing.T) {
				testIterOrdinalRange(t, prollyMap.(Map), tuples)
			})

			indexMap, tuples2 := makeProllySecondaryIndex(t, s)
			t.Run("iter prefix range", func(t *testing.T) {
				testIterPrefixRange(t, indexMap, tuples2)
			})

			pm := prollyMap.(Map)
			t.Run("tuple exists in map", func(t *testing.T) {
				testHas(t, pm, tuples)
			})

			t.Run("walk addresses smoke test", func(t *testing.T) {
				err := pm.WalkAddresses(ctx, func(_ context.Context, addr hash.Hash) error {
					assert.True(t, addr != hash.Hash{})
					return nil
				})
				assert.NoError(t, err)
			})
			t.Run("walk nodes smoke test", func(t *testing.T) {
				err := pm.WalkNodes(ctx, func(_ context.Context, nd tree.Node) error {
					assert.True(t, nd.Count() > 1)
					return nil
				})
				assert.NoError(t, err)
			})
		})
	}
}

func TestMutateMapWithTupleIter(t *testing.T) {
	ctx := context.Background()
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)
	ns := tree.NewTestNodeStore()

	scales := []int{
		20,
		200,
		2000,
		20_000,
	}

	for _, s := range scales {
		t.Run("scale "+strconv.Itoa(s), func(t *testing.T) {
			all := tree.RandomTuplePairs(ctx, s, kd, vd, ns)

			// randomize |all| and partition
			rand.Shuffle(s, func(i, j int) {
				all[i], all[j] = all[j], all[i]
			})
			q1, q2, q3 := s/4, (s*2)/4, (s*3)/4

			// unchanged tuples
			statics := make([][2]val.Tuple, s/4)
			copy(statics, all[:q1])
			tree.SortTuplePairs(ctx, statics, kd)

			// tuples to be updated
			updates := make([][2]val.Tuple, s/4)
			copy(updates, all[q1:q2])
			rand.Shuffle(len(updates), func(i, j int) {
				// shuffle values relative to keys
				updates[i][1], updates[j][1] = updates[j][1], updates[i][1]
			})
			tree.SortTuplePairs(ctx, updates, kd)

			// tuples to be deleted
			deletes := make([][2]val.Tuple, s/4)
			copy(deletes, all[q2:q3])
			for i := range deletes {
				deletes[i][1] = nil
			}
			tree.SortTuplePairs(ctx, deletes, kd)

			// tuples to be inserted
			inserts := make([][2]val.Tuple, s/4)
			copy(inserts, all[q3:])
			tree.SortTuplePairs(ctx, inserts, kd)

			var mutations [][2]val.Tuple
			mutations = append(mutations, inserts...)
			mutations = append(mutations, updates...)
			mutations = append(mutations, deletes...)
			tree.SortTuplePairs(ctx, mutations, kd)

			// original tuples, before modification
			base := all[:q3]
			tree.SortTuplePairs(ctx, base, kd)
			before := mustProllyMapFromTuples(t, kd, vd, base, ns)

			ds, err := DebugFormat(ctx, before)
			assert.NoError(t, err)
			assert.NotNil(t, ds)

			for _, kv := range statics {
				ok, err := before.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.True(t, ok)
				err = before.Get(ctx, kv[0], func(k, v val.Tuple) error {
					assert.Equal(t, k, kv[0])
					assert.Equal(t, v, kv[1])
					return nil
				})
				assert.NoError(t, err)
			}
			for _, kv := range inserts {
				ok, err := before.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.False(t, ok)
			}
			for _, kv := range updates {
				ok, err := before.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.NoError(t, err)
			}
			for _, kv := range deletes {
				ok, err := before.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.True(t, ok)
			}

			after, err := MutateMapWithTupleIter(ctx, before, &testTupleIter{tuples: mutations})
			require.NoError(t, err)
			for _, kv := range statics {
				ok, err := after.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.True(t, ok)
				err = after.Get(ctx, kv[0], func(k, v val.Tuple) error {
					assert.Equal(t, k, kv[0])
					assert.Equal(t, v, kv[1])
					return nil
				})
				assert.NoError(t, err)
			}
			for _, kv := range inserts {
				ok, err := after.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.True(t, ok)
				err = after.Get(ctx, kv[0], func(k, v val.Tuple) error {
					assert.Equal(t, k, kv[0])
					assert.Equal(t, v, kv[1])
					return nil
				})
				assert.NoError(t, err)
			}
			for _, kv := range updates {
				ok, err := after.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.True(t, ok)
				err = after.Get(ctx, kv[0], func(k, v val.Tuple) error {
					assert.Equal(t, k, kv[0])
					assert.Equal(t, v, kv[1])
					return nil
				})
				assert.NoError(t, err)
			}
			for _, kv := range deletes {
				ok, err := after.Has(ctx, kv[0])
				assert.NoError(t, err)
				assert.False(t, ok)
			}
		})
	}
}

func TestVisitMapLevelOrder(t *testing.T) {
	scales := []int{
		20,
		200,
		2000,
		20_000,
	}
	for _, s := range scales {
		t.Run("scale "+strconv.Itoa(s), func(t *testing.T) {
			ctx := context.Background()
			tm, _ := makeProllyMap(t, s)
			set1 := hash.NewHashSet()
			err := tm.(Map).WalkAddresses(ctx, func(ctx context.Context, addr hash.Hash) error {
				set1.Insert(addr)
				return nil
			})
			require.NoError(t, err)
			set2 := hash.NewHashSet()
			err = VisitMapLevelOrder(ctx, tm.(Map), func(h hash.Hash) (int64, error) {
				set2.Insert(h)
				return 0, nil
			})
			require.NoError(t, err)
			assert.Equal(t, set1.Size(), set2.Size())
			for h := range set1 {
				assert.True(t, set2.Has(h))
			}
		})
	}
}

func TestNewEmptyNode(t *testing.T) {
	s := message.NewProllyMapSerializer(val.TupleDesc{}, sharedPool)
	msg := s.Serialize(nil, nil, nil, 0)
	empty, fileId, err := tree.NodeFromBytes(msg)
	require.NoError(t, err)
	assert.Equal(t, fileId, serial.ProllyTreeNodeFileID)
	assert.Equal(t, 0, empty.Level())
	assert.Equal(t, 0, empty.Count())
	tc, err := empty.TreeCount()
	require.NoError(t, err)
	assert.Equal(t, 0, tc)
	assert.Equal(t, 76, empty.Size())
	assert.True(t, empty.IsLeaf())
}

// credit: https://github.com/tailscale/tailscale/commit/88586ec4a43542b758d6f4e15990573970fb4e8a
func TestMapGetAllocs(t *testing.T) {
	ctx := context.Background()
	m, tuples := makeProllyMap(t, 100_000)

	// assert no allocs for Map.Get()
	avg := testing.AllocsPerRun(100, func() {
		k := tuples[testRand.Intn(len(tuples))][0]
		_ = m.Get(ctx, k, func(key, val val.Tuple) (err error) {
			return
		})
	})
	assert.Equal(t, 0.0, avg)
}

func makeProllyMap(t *testing.T, count int) (testMap, [][2]val.Tuple) {
	ctx := context.Background()
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)
	ns := tree.NewTestNodeStore()

	tuples := tree.RandomTuplePairs(ctx, count, kd, vd, ns)
	om := mustProllyMapFromTuples(t, kd, vd, tuples, ns)

	return om, tuples
}

func makeProllySecondaryIndex(t *testing.T, count int) (testMap, [][2]val.Tuple) {
	ctx := context.Background()
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor()
	ns := tree.NewTestNodeStore()
	tuples := tree.RandomCompositeTuplePairs(ctx, count, kd, vd, ns)
	om := mustProllyMapFromTuples(t, kd, vd, tuples, ns)

	return om, tuples
}

func testGet(t *testing.T, om testMap, tuples [][2]val.Tuple) {
	ctx := context.Background()

	// test get
	for i, kv := range tuples {
		err := om.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			expKey, expVal := kv[0], kv[1]
			assert.Equal(t, key, expKey)
			assert.Equal(t, val, expVal)
			return
		})
		require.NoError(t, err)

		if m, ok := om.(Map); ok {
			ord, err := m.GetOrdinalForKey(ctx, kv[0])
			require.NoError(t, err)
			assert.Equal(t, uint64(i), ord)
		}
	}

	// test get with non-existent keys
	kd, vd := om.Descriptors()
	inserts := generateInserts(t, om, kd, vd, len(tuples)/2)
	for _, kv := range inserts {
		err := om.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.Equal(t, 0, len(key), "Got %s", kd.Format(ctx, key))
			assert.Equal(t, 0, len(val), "Got %s", vd.Format(ctx, val))
			return nil
		})
		require.NoError(t, err)

		if m, ok := om.(Map); ok {
			// find the expected ordinal return value for this non-existent key
			exp := len(tuples)
			for i := 0; i < len(tuples); i++ {
				if kd.Compare(ctx, tuples[i][0], kv[0]) >= 0 {
					exp = i
					break
				}
			}

			ord, err := m.GetOrdinalForKey(ctx, kv[0])
			require.NoError(t, err)
			assert.Equal(t, uint64(exp), ord)
		}
	}

	desc := keyDescFromMap(om)

	// test point lookup
	for _, kv := range tuples {
		rng := pointRangeFromTuple(kv[0], desc)
		require.True(t, rng.IsStrictKeyLookup(desc))

		iter, err := om.IterRange(ctx, rng)
		require.NoError(t, err)

		k, v, err := iter.Next(ctx)
		require.NoError(t, err)
		assert.Equal(t, kv[0], k)
		assert.Equal(t, kv[1], v)

		k, v, err = iter.Next(ctx)
		assert.Error(t, err, io.EOF)
		assert.Nil(t, k)
		assert.Nil(t, v)
	}
}

func testHas(t *testing.T, om Map, tuples [][2]val.Tuple) {
	ctx := context.Background()
	for _, kv := range tuples {
		ok, err := om.Has(ctx, kv[0])
		assert.True(t, ok)
		require.NoError(t, err)
	}
}

func testIterAll(t *testing.T, om testMap, tuples [][2]val.Tuple) {
	ctx := context.Background()
	iter, err := om.IterAll(ctx)
	require.NoError(t, err)

	actual := make([][2]val.Tuple, len(tuples)*2)

	idx := 0
	for {
		key, value, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		actual[idx][0], actual[idx][1] = key, value
		idx++
	}
	actual = actual[:idx]

	assert.Equal(t, len(tuples), idx)
	for i, kv := range actual {
		require.True(t, i < len(tuples))
		assert.Equal(t, tuples[i][0], kv[0])
		assert.Equal(t, tuples[i][1], kv[1])
	}
}

func pointRangeFromTuple(tup val.Tuple, desc val.TupleDesc) Range {
	ctx := context.Background()
	return closedRange(ctx, tup, tup, desc)
}

func formatTuples(tuples [][2]val.Tuple, kd, vd val.TupleDesc) string {
	ctx := context.Background()
	var sb strings.Builder
	sb.WriteString("Tuples (")
	sb.WriteString(strconv.Itoa(len(tuples)))
	sb.WriteString(") {\n")
	for _, kv := range tuples {
		sb.WriteString("\t")
		sb.WriteString(kd.Format(ctx, kv[0]))
		sb.WriteString(", ")
		sb.WriteString(vd.Format(ctx, kv[1]))
		sb.WriteString("\n")
	}
	sb.WriteString("}\n")
	return sb.String()
}

type testTupleIter struct {
	tuples [][2]val.Tuple
}

func (t *testTupleIter) Next(context.Context) (k, v val.Tuple) {
	if len(t.tuples) > 0 {
		k, v = t.tuples[0][0], t.tuples[0][1]
		t.tuples = t.tuples[1:]
	}
	return
}
