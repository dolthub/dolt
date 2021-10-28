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
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/dolthub/dolt/go/store/val"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	t.Run("get item from map", func(t *testing.T) {
		testMapGetItem(t, 10)
		testMapGetItem(t, 100)
		testMapGetItem(t, 1000)
		testMapGetItem(t, 10_000)
	})
}

func testMapGetItem(t *testing.T, count int) {
	ctx := context.Background()
	m, items := randomMap(t, count)

	for _, kv := range items {
		err := m.Get(ctx, kv[0], func(key, val val.Tuple) (err error) {
			assert.NotNil(t, kv[0])
			assert.Equal(t, kv[0], key)
			assert.Equal(t, kv[1], val)
			return
		})
		require.NoError(t, err)
	}
}

func randomMap(t *testing.T, count int) (Map, [][2]val.Tuple) {
	kd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: false},
	)
	vd := val.NewTupleDescriptor(
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
		val.Type{Enc: val.Int64Enc, Nullable: true},
	)

	return randomMapFromDescriptors(t, count, kd, vd)
}

func randomMapFromDescriptors(t *testing.T, count int, kd, vd val.TupleDesc) (Map, [][2]val.Tuple) {
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
	items = make([][2]val.Tuple, count/2)
	for i := range items {
		items[i][0] = randomTuple(keyDesc)
		items[i][1] = randomTuple(valDesc)
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

func randomTuple(desc val.TupleDesc) (tup val.Tuple) {
	tb := val.NewTupleBuilder(desc)
	for i, typ := range desc.Types {
		randomField(tb, i, typ)
	}
	return tb.Tuple(shared)
}

var src = rand.New(rand.NewSource(0))

func randomField(tb *val.TupleBuilder, idx int, typ val.Type) {
	// todo(andy): add NULLs

	neg := -1
	if src.Int()%2 == 1 {
		neg = 1
	}

	switch typ.Enc {
	case val.Int8Enc:
		v := int8(src.Intn(math.MaxInt8) * neg)
		tb.PutInt8(idx, v)
	case val.Uint8Enc:
		v := uint8(src.Intn(math.MaxUint8))
		tb.PutUint8(idx, v)
	case val.Int16Enc:
		v := int16(src.Intn(math.MaxInt16) * neg)
		tb.PutInt16(idx, v)
	case val.Uint16Enc:
		v := uint16(src.Intn(math.MaxUint16))
		tb.PutUint16(idx, v)
	case val.Int24Enc:
		panic("base24")
	case val.Uint24Enc:
		panic("base24")
	case val.Int32Enc:
		v := int32(src.Intn(math.MaxInt32) * neg)
		tb.PutInt32(idx, v)
	case val.Uint32Enc:
		v := uint32(src.Intn(math.MaxUint32))
		tb.PutUint32(idx, v)
	case val.Int64Enc:
		v := int64(src.Intn(math.MaxInt64) * neg)
		tb.PutInt64(idx, v)
	case val.Uint64Enc:
		v := uint64(src.Uint64())
		tb.PutUint64(idx, v)
	case val.Float32Enc:
		tb.PutFloat32(idx, src.Float32())
	case val.Float64Enc:
		tb.PutFloat64(idx, src.Float64())
	case val.StringEnc:
		buf := make([]byte, (src.Int63()%40)+10)
		src.Read(buf)
		tb.PutString(idx, string(buf))
	case val.BytesEnc:
		buf := make([]byte, (src.Int63()%40)+10)
		src.Read(buf)
		tb.PutBytes(idx, buf)
	default:
		panic("unknown encoding")
	}
}
