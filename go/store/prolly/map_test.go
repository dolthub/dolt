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
	"io"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/dolthub/dolt/go/store/val"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testRand = rand.New(rand.NewSource(0))

func TestMap(t *testing.T) {
	t.Run("get item from map", func(t *testing.T) {
		testMapGet(t, 10)
		testMapGet(t, 100)
		testMapGet(t, 1000)
		testMapGet(t, 10_000)
	})
	t.Run("get from map at index", func(t *testing.T) {
		testMapGetIndex(t, 10)
		testMapGetIndex(t, 100)
		testMapGetIndex(t, 1000)
		testMapGetIndex(t, 10_000)
	})
	//t.Run("get value range from map", func(t *testing.T) {
	//	testMapIterValueRange(t, 10)
	//	testMapIterValueRange(t, 100)
	//	testMapIterValueRange(t, 1000)
	//	testMapIterValueRange(t, 10_000)
	//})
	t.Run("get index range from map", func(t *testing.T) {
		testMapIterIndexRange(t, 10)
		testMapIterIndexRange(t, 100)
		testMapIterIndexRange(t, 1000)
		testMapIterIndexRange(t, 10_000)
	})
}

func testMapGet(t *testing.T, count int) {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	ctx := context.Background()
	m, kvPairs := randomMap(t, count, kd, vd)

	for _, kv := range kvPairs {
		ok, err := m.Has(ctx, kv[0])
		require.True(t, ok)
		require.NoError(t, err)
		err = m.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, kv[0], key)
			assert.Equal(t, kv[1], val)
			return
		})
		require.NoError(t, err)
	}
}

func testMapGetIndex(t *testing.T, count int) {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	ctx := context.Background()
	m, kvPairs := randomMap(t, count, kd, vd)

	for idx, kv := range kvPairs {
		err := m.GetIndex(ctx, uint64(idx), func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, kv[0], key)
			assert.Equal(t, kv[1], val)
			return
		})
		require.NoError(t, err)
	}
}

func testMapIterValueRange(t *testing.T, count int) {
	assert.Equal(t, count, count)
}

func testMapIterIndexRange(t *testing.T, count int) {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	m, kvPairs := randomMap(t, count, kd, vd)
	ranges := indexRanges(m)

	ctx := context.Background()
	for _, rng := range ranges {
		rng.reverse = false
		idx := rng.low
		iter, err := m.IterIndexRange(ctx, rng)
		require.NoError(t, err)
		for {
			key, value, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			assert.Equal(t, kvPairs[idx][0], key)
			assert.Equal(t, kvPairs[idx][1], value)
			idx++
		}
	}

	for _, rng := range ranges {
		rng.reverse = true
		idx := rng.high
		iter, err := m.IterIndexRange(ctx, rng)
		require.NoError(t, err)
		for {
			key, value, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			assert.Equal(t, kvPairs[idx][0], key)
			assert.Equal(t, kvPairs[idx][1], value)
			idx--
		}
	}
}

func indexRanges(m Map) (ranges []IndexRange) {
	ok := true
	start := uint64(0)
	for ok {
		width := (testRand.Uint64() % 15) + 1
		stop := start + width

		if stop >= m.Count() {
			stop = m.Count() - 1
			ok = false
		}

		ranges = append(ranges, IndexRange{
			low:  start,
			high: stop,
		})

		start = stop
	}
	return
}

func randomMap(t *testing.T, count int, kd, vd val.TupleDesc) (Map, [][2]val.Tuple) {
	ctx := context.Background()
	nrw := newTestNRW()
	chunker, err := newEmptyTreeChunker(ctx, nrw, newDefaultNodeSplitter)
	require.NoError(t, err)

	items := randomTuplePairs(count, kd, vd)
	for _, item := range items {
		_, err := chunker.Append(ctx, nodeItem(item[0]), nodeItem(item[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	m := Map{
		root:    root,
		keyDesc: kd,
		valDesc: vd,
		nrw:     nrw,
	}

	return m, items
}

func randomTuplePairs(count int, keyDesc, valDesc val.TupleDesc) (items [][2]val.Tuple) {
	keyBuilder := val.NewTupleBuilder(keyDesc)
	valBuilder := val.NewTupleBuilder(valDesc)

	items = make([][2]val.Tuple, count/2)
	for i := range items {
		items[i][0] = randomTuple(keyBuilder)
		items[i][1] = randomTuple(valBuilder)
	}

	sort.Slice(items, func(i, j int) bool {
		return keyDesc.Compare(items[i][0], items[j][0]) < 0
	})

	for i := range items {
		if i == 0 {
			continue
		}
		if keyDesc.Compare(items[i][0], items[i-1][0]) == 0 {
			panic("duplicate key")
		}
	}
	return
}

func randomTuple(tb *val.TupleBuilder) (tup val.Tuple) {
	for i, typ := range tb.Desc.Types {
		randomField(tb, i, typ)
	}
	return tb.Tuple(sharedPool)
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
	case val.Int24Enc:
		panic("24 bit")
	case val.Uint24Enc:
		panic("24 bit")
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
