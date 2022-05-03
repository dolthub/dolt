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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/pool"
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
	b := &testBuilder{
		keys:   keys,
		values: values,
		level:  0,
	}
	return b.Build(sharedPool)
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

func newTestBuilder(level int) *testBuilder {
	return &testBuilder{level: level}
}

var _ NodeBuilderFactory[*testBuilder] = newTestBuilder

type testBuilder struct {
	keys, values []Item
	size, level  int

	subtrees SubtreeCounts
}

var _ NodeBuilder = &testBuilder{}

func (tb *testBuilder) StartNode() {
	tb.reset()
}

func (tb *testBuilder) HasCapacity(key, value Item) bool {
	sum := tb.size + len(key) + len(value)
	return sum <= int(MaxVectorOffset)
}

func (tb *testBuilder) AddItems(key, value Item, subtree uint64) {
	tb.keys = append(tb.keys, key)
	tb.values = append(tb.values, value)
	tb.size += len(key) + len(value)
	tb.subtrees = append(tb.subtrees, subtree)
}

func (tb *testBuilder) Count() int {
	return len(tb.keys)
}

func (tb *testBuilder) Build(pool pool.BuffPool) (node Node) {
	var (
		keyTups, keyOffs fb.UOffsetT
		valTups, valOffs fb.UOffsetT
		refArr, cardArr  fb.UOffsetT
	)

	keySz, valSz, bufSz := measureNodeSize(tb.keys, tb.values, tb.subtrees)
	b := getMapBuilder(pool, bufSz)

	// serialize keys and offsets
	keyTups = writeItemBytes(b, tb.keys, keySz)
	serial.ProllyTreeNodeStartKeyOffsetsVector(b, len(tb.keys)-1)
	keyOffs = b.EndVector(writeItemOffsets(b, tb.keys, keySz))

	if tb.level == 0 {
		// serialize value tuples for leaf nodes
		valTups = writeItemBytes(b, tb.values, valSz)
		serial.ProllyTreeNodeStartValueOffsetsVector(b, len(tb.values)-1)
		valOffs = b.EndVector(writeItemOffsets(b, tb.values, valSz))
	} else {
		// serialize child refs and subtree counts for internal nodes
		refArr = writeItemBytes(b, tb.values, valSz)
		cardArr = writeCountArray(b, tb.subtrees)
	}

	// populate the node's vtable
	serial.ProllyTreeNodeStart(b)
	serial.ProllyTreeNodeAddKeyItems(b, keyTups)
	serial.ProllyTreeNodeAddKeyOffsets(b, keyOffs)
	if tb.level == 0 {
		serial.ProllyTreeNodeAddValueItems(b, valTups)
		serial.ProllyTreeNodeAddValueOffsets(b, valOffs)
		serial.ProllyTreeNodeAddTreeCount(b, uint64(len(tb.keys)))
	} else {
		serial.ProllyTreeNodeAddAddressArray(b, refArr)
		serial.ProllyTreeNodeAddSubtreeCounts(b, cardArr)
		serial.ProllyTreeNodeAddTreeCount(b, tb.subtrees.Sum())
	}
	serial.ProllyTreeNodeAddKeyType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddValueType(b, serial.ItemTypeTupleFormatAlpha)
	serial.ProllyTreeNodeAddTreeLevel(b, uint8(tb.level))
	b.Finish(serial.ProllyTreeNodeEnd(b))
	tb.reset()

	buf := b.FinishedBytes()
	return NodeFromBytes(buf)
}

func (tb *testBuilder) reset() {
	// buffers are copied, it's safe to re-use the memory.
	tb.keys = tb.keys[:0]
	tb.values = tb.values[:0]
	tb.size = 0
	tb.subtrees = tb.subtrees[:0]
}

func getMapBuilder(pool pool.BuffPool, sz int) (b *fb.Builder) {
	b = fb.NewBuilder(0)
	buf := pool.Get(uint64(sz))
	b.Bytes = buf[:0]
	return
}

// measureNodeSize returns the exact Size of the tuple vectors for keys and values,
// and an estimate of the overall Size of the final flatbuffer.
func measureNodeSize(keys, values []Item, subtrees []uint64) (keySz, valSz, bufSz int) {
	for i := range keys {
		keySz += len(keys[i])
		valSz += len(values[i])
	}
	refCntSz := len(subtrees) * binary.MaxVarintLen64

	// constraints enforced upstream
	if keySz > int(MaxVectorOffset) {
		panic(fmt.Sprintf("key vector exceeds Size limit ( %d > %d )", keySz, MaxVectorOffset))
	}
	if valSz > int(MaxVectorOffset) {
		panic(fmt.Sprintf("value vector exceeds Size limit ( %d > %d )", valSz, MaxVectorOffset))
	}

	bufSz += keySz + valSz               // tuples
	bufSz += refCntSz                    // subtree counts
	bufSz += len(keys)*2 + len(values)*2 // offsets
	bufSz += 8 + 1 + 1 + 1               // metadata
	bufSz += 72                          // vtable (approx)

	return
}

func writeItemBytes(b *fb.Builder, items []Item, sumSz int) fb.UOffsetT {
	b.Prep(fb.SizeUOffsetT, sumSz)

	stop := int(b.Head())
	start := stop - sumSz
	for _, item := range items {
		copy(b.Bytes[start:stop], item)
		start += len(item)
	}

	start = stop - sumSz
	return b.CreateByteVector(b.Bytes[start:stop])
}

func writeItemOffsets(b *fb.Builder, items []Item, sz int) (cnt int) {
	off := sz
	for i := len(items) - 1; i > 0; i-- { // omit first offset
		off -= len(items[i])
		b.PrependUint16(uint16(off))
		cnt++
	}
	return
}

func writeCountArray(b *fb.Builder, sc SubtreeCounts) fb.UOffsetT {
	// todo(andy) write without copy
	arr := WriteSubtreeCounts(sc)
	return b.CreateByteVector(arr)
}
