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
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/val"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripInts(t *testing.T) {
	tups, _ := ascendingUintTuples(10)
	keys := make([]val.Tuple, len(tups))
	values := make([]val.Tuple, len(tups))
	for i := range tups {
		keys[i] = tups[i][0]
		values[i] = tups[i][1]
	}
	require.True(t, sumTupleSize(keys)+sumTupleSize(values) < maxVectorOffset)

	nd := newTupleLeafNode(keys, values)
	assert.True(t, nd.leafNode())
	assert.Equal(t, len(keys), int(nd.count))
	for i := range keys {
		assert.Equal(t, keys[i], val.Tuple(nd.getKey(i)))
		assert.Equal(t, values[i], val.Tuple(nd.getValue(i)))
	}
}

func TestRoundTripNodeItems(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		keys, values := randomNodeItemPairs(t, (rand.Int()%101)+50)
		require.True(t, sumSize(keys)+sumSize(values) < maxVectorOffset)

		nd := newLeafNode(keys, values)
		assert.True(t, nd.leafNode())
		assert.Equal(t, len(keys), int(nd.count))
		for i := range keys {
			assert.Equal(t, keys[i], nd.getKey(i))
			assert.Equal(t, values[i], nd.getValue(i))
		}
	}
}

func TestGetKeyValueOffsetsVectors(t *testing.T) {
	for trial := 0; trial < 100; trial++ {
		keys, values := randomNodeItemPairs(t, (rand.Int()%101)+50)
		require.True(t, sumSize(keys)+sumSize(values) < maxVectorOffset)
		nd := newLeafNode(keys, values)

		ko1, vo1 := offsetsFromSlicedBuffers(nd.keys, nd.values)
		ko2, vo2 := offsetsFromFlatbuffer(nd.buf)

		assert.Equal(t, len(ko1), len(ko2))
		assert.Equal(t, len(ko1), len(keys)-1)
		assert.Equal(t, ko1, ko2)

		assert.Equal(t, len(vo1), len(vo2))
		assert.Equal(t, len(vo1), len(values)-1)
		assert.Equal(t, vo1, vo2)

	}
}

func TestNodeSize(t *testing.T) {
	sz := unsafe.Sizeof(Node{})
	assert.Equal(t, 136, int(sz))
}

func TestCountArray(t *testing.T) {
	for k := 0; k < 100; k++ {
		n := testRand.Intn(45) + 5

		counts := make(subtreeCounts, n)
		sum := uint64(0)
		for i := range counts {
			c := testRand.Uint64() % math.MaxUint32
			counts[i] = c
			sum += c
		}
		assert.Equal(t, sum, counts.sum())

		// round trip the array
		buf := writeSubtreeCounts(counts)
		counts = readSubtreeCounts(n, buf)
		assert.Equal(t, sum, counts.sum())
	}
}

func newLeafNode(keys, values []NodeItem) Node {
	b := &nodeBuilder{
		keys:   keys,
		values: values,
		level:  0,
	}
	return b.build(sharedPool)
}

func newTupleLeafNode(keys, values []val.Tuple) Node {
	ks := make([]NodeItem, len(keys))
	for i := range ks {
		ks[i] = NodeItem(keys[i])
	}
	vs := make([]NodeItem, len(values))
	for i := range vs {
		vs[i] = NodeItem(values[i])
	}
	return newLeafNode(ks, vs)
}

func randomNodeItemPairs(t *testing.T, count int) (keys, values []NodeItem) {
	keys = make([]NodeItem, count)
	for i := range keys {
		sz := (rand.Int() % 41) + 10
		keys[i] = make(NodeItem, sz)
		_, err := rand.Read(keys[i])
		assert.NoError(t, err)
	}

	values = make([]NodeItem, count)
	copy(values, keys)
	rand.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	return
}

// Map<Tuple<Uint32>,Tuple<Uint32>>
func ascendingUintTuples(count int) (tuples [][2]val.Tuple, desc val.TupleDesc) {
	desc = val.NewTupleDescriptor(val.Type{Enc: val.Uint32Enc})
	bld := val.NewTupleBuilder(desc)
	tuples = make([][2]val.Tuple, count)
	for i := range tuples {
		bld.PutUint32(0, uint32(i))
		tuples[i][0] = bld.Build(sharedPool)
		bld.PutUint32(0, uint32(i+count))
		tuples[i][1] = bld.Build(sharedPool)
	}
	return
}

func ascendingIntTuples(t *testing.T, count int) (tuples [][2]val.Tuple, desc val.TupleDesc) {
	desc = val.NewTupleDescriptor(val.Type{Enc: val.Int32Enc})
	bld := val.NewTupleBuilder(desc)
	tuples = make([][2]val.Tuple, count)
	for i := range tuples {
		bld.PutInt32(0, int32(i))
		tuples[i][0] = bld.Build(sharedPool)
		bld.PutInt32(0, int32(i+count))
		tuples[i][1] = bld.Build(sharedPool)
	}
	return
}

// Map<Tuple<Uint32,Uint32>,Tuple<Uint32,Uint32>>
func ascendingCompositeIntTuples(count int) (keys, values []val.Tuple, desc val.TupleDesc) {
	desc = val.NewTupleDescriptor(val.Type{Enc: val.Uint32Enc}, val.Type{Enc: val.Uint32Enc})
	bld := val.NewTupleBuilder(desc)

	tups := make([]val.Tuple, count*2)
	for i := range tups {
		bld.PutUint32(0, uint32(i))
		bld.PutUint32(1, uint32(i))
		tups[i] = bld.Build(sharedPool)
	}
	keys, values = tups[:count], tups[count:]
	return
}

func sumSize(items []NodeItem) (sz uint64) {
	for _, item := range items {
		sz += uint64(len(item))
	}
	return
}

func sumTupleSize(items []val.Tuple) (sz uint64) {
	for _, item := range items {
		sz += uint64(len(item))
	}
	return
}

func offsetsFromFlatbuffer(buf serial.TupleMap) (ko, vo []uint16) {
	ko = make([]uint16, buf.KeyOffsetsLength())
	for i := range ko {
		ko[i] = buf.KeyOffsets(i)
	}

	vo = make([]uint16, buf.ValueOffsetsLength())
	for i := range vo {
		vo[i] = buf.ValueOffsets(i)
	}

	return
}

func offsetsFromSlicedBuffers(keys, values val.SlicedBuffer) (ko, vo []uint16) {
	ko = deserializeOffsets(keys.Offs)
	vo = deserializeOffsets(values.Offs)
	return
}

func deserializeOffsets(buf []byte) (offs []uint16) {
	offs = make([]uint16, len(buf)/2)
	for i := range offs {
		start, stop := i*2, (i+1)*2
		offs[i] = binary.LittleEndian.Uint16(buf[start:stop])
	}
	return
}
