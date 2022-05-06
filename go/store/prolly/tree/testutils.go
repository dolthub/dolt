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

package tree

import (
	"math"
	"math/rand"
	"sort"

	"github.com/dolthub/dolt/go/store/prolly/message"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/val"
)

var testRand = rand.New(rand.NewSource(1))

func NewTestNodeStore() NodeStore {
	ts := &chunks.TestStorage{}
	return NewNodeStore(ts.NewView())
}

func NewTupleLeafNode(keys, values []val.Tuple) Node {
	ks := make([]Item, len(keys))
	for i := range ks {
		ks[i] = Item(keys[i])
	}
	vs := make([]Item, len(values))
	for i := range vs {
		vs[i] = Item(values[i])
	}
	return newLeafNode(ks, vs)
}

func RandomTuplePairs(count int, keyDesc, valDesc val.TupleDesc) (items [][2]val.Tuple) {
	keyBuilder := val.NewTupleBuilder(keyDesc)
	valBuilder := val.NewTupleBuilder(valDesc)

	items = make([][2]val.Tuple, count)
	for i := range items {
		items[i][0] = RandomTuple(keyBuilder)
		items[i][1] = RandomTuple(valBuilder)
	}

	dupes := make([]int, 0, count)
	for {
		SortTuplePairs(items, keyDesc)
		for i := range items {
			if i == 0 {
				continue
			}
			if keyDesc.Compare(items[i][0], items[i-1][0]) == 0 {
				dupes = append(dupes, i)
			}
		}
		if len(dupes) == 0 {
			break
		}

		// replace duplicates and validate again
		for _, d := range dupes {
			items[d][0] = RandomTuple(keyBuilder)
		}
		dupes = dupes[:0]
	}
	return items
}

func RandomCompositeTuplePairs(count int, keyDesc, valDesc val.TupleDesc) (items [][2]val.Tuple) {
	// preconditions
	if count%5 != 0 {
		panic("expected empty divisible by 5")
	}
	if len(keyDesc.Types) < 2 {
		panic("expected composite key")
	}

	tt := RandomTuplePairs(count, keyDesc, valDesc)

	tuples := make([][2]val.Tuple, len(tt)*3)
	for i := range tuples {
		j := i % len(tt)
		tuples[i] = tt[j]
	}

	// permute the second column
	swap := make([]byte, len(tuples[0][0].GetField(1)))
	rand.Shuffle(len(tuples), func(i, j int) {
		f1 := tuples[i][0].GetField(1)
		f2 := tuples[i][0].GetField(1)
		copy(swap, f1)
		copy(f1, f2)
		copy(f2, swap)
	})

	SortTuplePairs(tuples, keyDesc)

	tuples = deduplicateTuples(keyDesc, tuples)

	return tuples[:count]
}

// Map<Tuple<Uint32>,Tuple<Uint32>>
func AscendingUintTuples(count int) (tuples [][2]val.Tuple, desc val.TupleDesc) {
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

func RandomTuple(tb *val.TupleBuilder) (tup val.Tuple) {
	for i, typ := range tb.Desc.Types {
		randomField(tb, i, typ)
	}
	return tb.Build(sharedPool)
}

func CloneRandomTuples(items [][2]val.Tuple) (clone [][2]val.Tuple) {
	clone = make([][2]val.Tuple, len(items))
	for i := range clone {
		clone[i] = items[i]
	}
	return
}

func SortTuplePairs(items [][2]val.Tuple, keyDesc val.TupleDesc) {
	sort.Slice(items, func(i, j int) bool {
		return keyDesc.Compare(items[i][0], items[j][0]) < 0
	})
}

func ShuffleTuplePairs(items [][2]val.Tuple) {
	testRand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
}

func newLeafNode(keys, values []Item) Node {
	kk := make([][]byte, len(keys))
	for i := range keys {
		kk[i] = keys[i]
	}
	vv := make([][]byte, len(values))
	for i := range vv {
		vv[i] = values[i]
	}

	s := message.ProllyMapSerializer{Pool: sharedPool}
	msg := s.Serialize(kk, vv, nil, 0)
	return NodeFromBytes(msg)
}

// assumes a sorted list
func deduplicateTuples(desc val.TupleDesc, tups [][2]val.Tuple) (uniq [][2]val.Tuple) {
	uniq = make([][2]val.Tuple, 1, len(tups))
	uniq[0] = tups[0]

	for i := 1; i < len(tups); i++ {
		cmp := desc.Compare(tups[i-1][0], tups[i][0])
		if cmp < 0 {
			uniq = append(uniq, tups[i])
		}
	}
	return
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
	case val.ByteStringEnc:
		buf := make([]byte, (testRand.Int63()%40)+10)
		testRand.Read(buf)
		tb.PutByteString(idx, buf)
	default:
		panic("unknown encoding")
	}
}
