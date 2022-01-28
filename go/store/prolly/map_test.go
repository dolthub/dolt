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
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

var testRand = rand.New(rand.NewSource(1))

func TestMap(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10_000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test proCur map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			prollyMap, tuples := makeProllyMap(t, s)

			t.Run("get item from map", func(t *testing.T) {
				testOrderedMapGet(t, prollyMap, tuples)
			})
			t.Run("iter all from map", func(t *testing.T) {
				testOrderedMapIterAll(t, prollyMap, tuples)
			})
			t.Run("iter all backwards from map", func(t *testing.T) {
				testOrderedMapIterAllBackward(t, prollyMap, tuples)
			})
			t.Run("iter value range", func(t *testing.T) {
				testOrderedMapIterValueRange(t, prollyMap, tuples)
			})

			pm := prollyMap.(Map)
			t.Run("item exists in map", func(t *testing.T) {
				testProllyMapHas(t, pm, tuples)
			})
		})
	}
}

func makeProllyMap(t *testing.T, count int) (orderedMap, [][2]val.Tuple) {
	ctx := context.Background()
	ns := newTestNodeStore()

	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
		val.Type{Enc: val.Uint32Enc, Nullable: true},
	)

	tuples := randomTuplePairs(count, kd, vd)

	chunker, err := newEmptyTreeChunker(ctx, ns, newDefaultNodeSplitter)
	require.NoError(t, err)

	for _, pair := range tuples {
		_, err := chunker.Append(ctx, nodeItem(pair[0]), nodeItem(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	m := Map{
		root:    root,
		keyDesc: kd,
		valDesc: vd,
		ns:      ns,
	}

	return m, tuples
}

type orderedMap interface {
	Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error)
	IterAll(ctx context.Context) (MapRangeIter, error)
	IterRange(ctx context.Context, rng Range) (MapRangeIter, error)
}

var _ orderedMap = Map{}
var _ orderedMap = MutableMap{}
var _ orderedMap = memoryMap{}

func getKeyDesc(om orderedMap) val.TupleDesc {
	switch m := om.(type) {
	case Map:
		return m.keyDesc
	case MutableMap:
		return m.m.keyDesc
	case memoryMap:
		return m.keyDesc
	default:
		panic("unknown ordered map")
	}
}

func testOrderedMapGet(t *testing.T, om orderedMap, tuples [][2]val.Tuple) {
	ctx := context.Background()
	for _, kv := range tuples {
		err := om.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, kv[0], key)
			assert.Equal(t, kv[1], val)
			return
		})
		require.NoError(t, err)
	}
}

func testProllyMapHas(t *testing.T, om Map, tuples [][2]val.Tuple) {
	ctx := context.Background()
	for _, kv := range tuples {
		ok, err := om.Has(ctx, kv[0])
		assert.True(t, ok)
		require.NoError(t, err)
	}
}

func testOrderedMapIterAll(t *testing.T, om orderedMap, tuples [][2]val.Tuple) {
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
		require.NoError(t, err)
		assert.Equal(t, tuples[i][0], kv[0])
		assert.Equal(t, tuples[i][1], kv[1])
	}
}

func testOrderedMapIterAllBackward(t *testing.T, om orderedMap, tuples [][2]val.Tuple) {
	desc := getKeyDesc(om)
	rng := Range{
		Start:   RangeCut{Unbound: true},
		Stop:    RangeCut{Unbound: true},
		KeyDesc: desc,
		Reverse: true,
	}

	ctx := context.Background()
	iter, err := om.IterRange(ctx, rng)
	require.NoError(t, err)

	idx := len(tuples) - 1
	for {
		key, value, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		assert.Equal(t, tuples[idx][0], key)
		assert.Equal(t, tuples[idx][1], value)
		idx--
	}
	assert.Equal(t, -1, idx)
}

type rangeTest struct {
	name      string
	testRange Range
	expCount  int
}

func testOrderedMapIterValueRange(t *testing.T, om orderedMap, tuples [][2]val.Tuple) {
	ctx := context.Background()
	desc := getKeyDesc(om)

	for i := 0; i < 100; i++ {

		cnt := len(tuples)
		a, z := testRand.Intn(cnt), testRand.Intn(cnt)
		if a > z {
			a, z = z, a
		}
		start, stop := tuples[a][0], tuples[z][0]

		tests := []rangeTest{
			// two-sided ranges
			{
				name:      "OpenRange",
				testRange: OpenRange(start, stop, desc),
				expCount:  nonNegative((z - a) - 1),
			},
			{
				name:      "OpenStartRange",
				testRange: OpenStartRange(start, stop, desc),
				expCount:  z - a,
			},
			{
				name:      "OpenStopRange",
				testRange: OpenStopRange(start, stop, desc),
				expCount:  z - a,
			},
			{
				name:      "ClosedRange",
				testRange: ClosedRange(start, stop, desc),
				expCount:  (z - a) + 1,
			},

			// put it down flip it and reverse it
			{
				name:      "OpenRange",
				testRange: OpenRange(stop, start, desc),
				expCount:  nonNegative((z - a) - 1),
			},
			{
				name:      "OpenStartRange",
				testRange: OpenStartRange(stop, start, desc),
				expCount:  z - a,
			},
			{
				name:      "OpenStopRange",
				testRange: OpenStopRange(stop, start, desc),
				expCount:  z - a,
			},
			{
				name:      "ClosedRange",
				testRange: ClosedRange(stop, start, desc),
				expCount:  (z - a) + 1,
			},

			// one-sided ranges
			{
				name:      "GreaterRange",
				testRange: GreaterRange(start, desc),
				expCount:  nonNegative(cnt - a - 1),
			},
			{
				name:      "GreaterOrEqualRange",
				testRange: GreaterOrEqualRange(start, desc),
				expCount:  cnt - a,
			},
			{
				name:      "LesserRange",
				testRange: LesserRange(stop, desc),
				expCount:  z,
			},
			{
				name:      "LesserOrEqualRange",
				testRange: LesserOrEqualRange(stop, desc),
				expCount:  z + 1,
			},
		}

		for _, test := range tests {
			iter, err := om.IterRange(ctx, test.testRange)
			require.NoError(t, err)

			key, _, err := iter.Next(ctx)
			actCount := 0
			for err != io.EOF {
				actCount++
				prev := key
				key, _, err = iter.Next(ctx)

				if key != nil {
					if test.testRange.Reverse {
						assert.True(t, desc.Compare(prev, key) > 0)
					} else {
						assert.True(t, desc.Compare(prev, key) < 0)
					}
				}
			}
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, test.expCount, actCount)
			//fmt.Printf("a: %d \t z: %d cnt: %d", a, z, cnt)
		}
	}
}

func randomTuplePairs(count int, keyDesc, valDesc val.TupleDesc) (items [][2]val.Tuple) {
	keyBuilder := val.NewTupleBuilder(keyDesc)
	valBuilder := val.NewTupleBuilder(valDesc)

	items = make([][2]val.Tuple, count)
	for i := range items {
		items[i][0] = randomTuple(keyBuilder)
		items[i][1] = randomTuple(valBuilder)
	}

	sortTuplePairs(items, keyDesc)

	for i := range items {
		if i == 0 {
			continue
		}
		if keyDesc.Compare(items[i][0], items[i-1][0]) == 0 {
			panic("duplicate key, unlucky!")
		}
	}
	return
}

func randomTuple(tb *val.TupleBuilder) (tup val.Tuple) {
	for i, typ := range tb.Desc.Types {
		randomField(tb, i, typ)
	}
	return tb.Build(sharedPool)
}

func cloneRandomTuples(items [][2]val.Tuple) (clone [][2]val.Tuple) {
	clone = make([][2]val.Tuple, len(items))
	for i := range clone {
		clone[i] = items[i]
	}
	return
}

func sortTuplePairs(items [][2]val.Tuple, keyDesc val.TupleDesc) {
	sort.Slice(items, func(i, j int) bool {
		return keyDesc.Compare(items[i][0], items[j][0]) < 0
	})
}

func shuffleTuplePairs(items [][2]val.Tuple) {
	testRand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
}

func randomField(tb *val.TupleBuilder, idx int, typ val.Type) {
	// todo(andy): add NULLs

	neg := -1
	if testRand.Int()%2 == 1 {
		neg = 1
	}

	switch typ.Enc {
	case val.Int8Enc:
		v := int8(testRand.Intn(math.MaxInt8) * neg)
		tb.PutInt8(idx, v)
	case val.Uint8Enc:
		v := uint8(testRand.Intn(math.MaxUint8))
		tb.PutUint8(idx, v)
	case val.Int16Enc:
		v := int16(testRand.Intn(math.MaxInt16) * neg)
		tb.PutInt16(idx, v)
	case val.Uint16Enc:
		v := uint16(testRand.Intn(math.MaxUint16))
		tb.PutUint16(idx, v)
	case val.Int32Enc:
		v := int32(testRand.Intn(math.MaxInt32) * neg)
		tb.PutInt32(idx, v)
	case val.Uint32Enc:
		v := uint32(testRand.Intn(math.MaxUint32))
		tb.PutUint32(idx, v)
	case val.Int64Enc:
		v := int64(testRand.Intn(math.MaxInt64) * neg)
		tb.PutInt64(idx, v)
	case val.Uint64Enc:
		v := uint64(testRand.Uint64())
		tb.PutUint64(idx, v)
	case val.Float32Enc:
		tb.PutFloat32(idx, testRand.Float32())
	case val.Float64Enc:
		tb.PutFloat64(idx, testRand.Float64())
	case val.StringEnc:
		buf := make([]byte, (testRand.Int63()%40)+10)
		testRand.Read(buf)
		tb.PutString(idx, string(buf))
	case val.BytesEnc:
		buf := make([]byte, (testRand.Int63()%40)+10)
		testRand.Read(buf)
		tb.PutBytes(idx, buf)
	default:
		panic("unknown encoding")
	}
}

func nonNegative(x int) int {
	if x < 0 {
		x = 0
	}
	return x
}

func fmtTupleList(tuples [][2]val.Tuple, kd, vd val.TupleDesc) string {
	var sb strings.Builder
	sb.WriteString("{ ")
	for _, kv := range tuples {
		if kv[0] == nil || kv[1] == nil {
			break
		}
		sb.WriteString(kd.Format(kv[0]))
		sb.WriteString(": ")
		sb.WriteString(vd.Format(kv[1]))
		sb.WriteString(", ")
	}
	sb.WriteString("}")
	return sb.String()
}
