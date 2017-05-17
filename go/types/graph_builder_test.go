// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestGraphBuilderFindIndex(t *testing.T) {
	assert := assert.New(t)

	elems := []*graphStackElem{
		{key: String("ROOT")},
		{key: String("one")},
		{key: String("two")},
		{key: String("three")},
		{key: String("four")},
	}

	s := graphStack{elems: elems}
	assert.Equal(0, commonPrefixCount(s, []Value{String("zero")}))
	assert.Equal(1, commonPrefixCount(s, []Value{String("one"), String("zero")}))
	assert.Equal(3, commonPrefixCount(s, []Value{String("one"), String("two"), String("three")}))
	assert.Equal(-1, commonPrefixCount(s, []Value{String("one"), String("two"), String("three"), String("four")}))
	assert.Equal(4, commonPrefixCount(s, []Value{String("one"), String("two"), String("three"), String("four"), String("five")}))

	values := []Value{String("one"), String("two"), String("three"), String("four")}

	assert.Equal(-1, commonPrefixCount(graphStack{elems: elems[:1]}, []Value{}))
	assert.Equal(0, commonPrefixCount(graphStack{elems: elems[:1]}, values))
	assert.Equal(1, commonPrefixCount(graphStack{elems: elems[:2]}, values))
	assert.Equal(3, commonPrefixCount(graphStack{elems: elems[:4]}, values))
	assert.Equal(-1, commonPrefixCount(graphStack{elems: elems}, values))
	assert.Equal(2, commonPrefixCount(graphStack{elems: elems[:5]}, values[:2]))
}

type testGraphOp struct {
	keys ValueSlice
	kind NomsKind
	item sequenceItem
}

func SafeEquals(v1, v2 Value) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	return v1.Equals(v2)
}

func TestGraphBuilderEncodeDecodeAsKey(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()
	defer vrw.Close()

	struct1 := NewStruct("teststruct", StructData{
		"f1": String("v1"),
		"f2": String("v2"),
	})

	keys := ValueSlice{Bool(true), Number(19), String("think!"), struct1}
	byteBuf := [initialBufferSize]byte{}
	bs := byteBuf[:0]
	numKeys := len(keys)
	expectedRes := ValueSlice{}
	for _, k := range keys {
		if isKindOrderedByValue(k.Kind()) {
			expectedRes = append(expectedRes, k)
		} else {
			expectedRes = append(expectedRes, nil)
		}
		bs = encodeGraphKey(bs, k, vrw)
	}
	res := ValueSlice{}
	for pos := 0; pos < numKeys; pos++ {
		var k Value
		bs, k = decodeValue(bs, false, vrw)
		res = append(res, k)
	}

	assert.Equal(len(keys), len(res))
	for i, origKey := range expectedRes {
		assert.True(SafeEquals(origKey, res[i]))
	}
}

func TestGraphBuilderEncodeDecodeAsValue(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()
	defer vrw.Close()

	struct1 := NewStruct("teststruct", StructData{
		"f1": String("v1"),
		"f2": String("v2"),
	})

	keys := ValueSlice{Bool(true), Number(19), String("think!"), struct1}
	byteBuf := [initialBufferSize]byte{}
	bs := byteBuf[:0]
	numKeys := len(keys)
	for _, k := range keys {
		bs = encodeGraphValue(bs, k, vrw)
	}
	res := ValueSlice{}
	for pos := 0; pos < numKeys; pos++ {
		var k Value
		bs, k = decodeValue(bs, true, vrw)
		res = append(res, k)
	}

	assert.Equal(len(keys), len(res))
	for i, origKey := range keys {
		assert.True(SafeEquals(origKey, res[i]))
	}
}

func TestGraphBuilderMapSetGraphOp(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	opcStore := newLdbOpCacheStore(vs)
	opc := opcStore.opCache()
	defer opcStore.destroy()

	struct1 := NewStruct("teststruct", StructData{
		"f1": String("v1"),
		"f2": String("v2"),
	})
	keys := ValueSlice{Bool(true), Number(19), String("think!"), struct1}
	opc.GraphMapSet(keys, String("yo"), Number(199))
	iter := opc.NewIterator()
	assert.True(iter.Next())

	keys1, kind, item := iter.GraphOp()
	assert.Equal(len(keys), len(keys1))
	assert.True(keys.Equals(keys1))
	assert.Equal(MapKind, kind)
	assert.IsType(mapEntry{}, item)
	me := item.(mapEntry)
	assert.True(String("yo").Equals(me.key))
	assert.True(Number(199).Equals(me.value))

	assert.False(iter.Next())
}

// createTestMap() constructs a graph sized according to the |levels| and
// |avgSize| parameters. The graph will contain nested maps with a
// depth == |levels|, each map will contain |avgSize| elements of different
// types.
func createTestMap(levels, avgSize int, valGen func() Value) Map {
	sampleSize := func() int {
		size := (int(rand.Int31()) % avgSize) + (avgSize / 2)
		if size < 2 {
			return 2
		}
		return size
	}

	genLeaf := func() Value {
		numElems := sampleSize()
		elems := ValueSlice{}
		for i := 0; i < numElems; i++ {
			elems = append(elems, valGen())
		}
		switch rand.Int31() % 3 {
		case 0:
			if numElems%2 != 0 {
				numElems -= 1
			}
			return NewMap(elems[:numElems]...)
		case 1:
			return NewSet(elems...)
		case 2:
			return NewList(elems...)
		}
		panic("unreachable")
	}

	var genChildren func(lvl int) Map
	genChildren = func(lvl int) Map {
		numChildren := sampleSize()
		kvs := ValueSlice{}
		for i := 0; i < numChildren; i++ {
			if lvl == levels {
				kvs = append(kvs, valGen(), genLeaf())
			} else {
				// Once in a while, throw in a non-collection value into the
				// middle of the graph
				if rand.Int31()%10 == 0 {
					kvs = append(kvs, valGen(), valGen())
				} else {
					kvs = append(kvs, valGen(), genChildren(lvl+1))
				}
			}
		}
		return NewMap(kvs...)
	}

	return genChildren(0)
}

// valGen() creates a random String, Number, or Struct Value
func valGen() Value {
	num := rand.Int31() % 1000000
	switch rand.Int31() % 4 {
	case 0:
		return String(fmt.Sprintf("%d", num))
	case 1:
		return Number(num)
	case 2:
		return NewStruct("teststruct", map[string]Value{"f1": Number(num)})
	case 3:
		return NewStruct("teststruct", map[string]Value{"f1": String(fmt.Sprintf("%d", num))})
	}
	panic("unreachable")
}

// dupSlice() duplicates a slice along with it's backing store.
func dupSlice(s ValueSlice) ValueSlice {
	vs := make(ValueSlice, len(s))
	copy(vs, s)
	return vs
}

func shuffle(a []testGraphOp) {
	for i := range a {
		j := rand.Intn(i + 1)
		if a[i].kind != ListKind && a[j].kind != ListKind {
			a[i], a[j] = a[j], a[i]
		}
	}
}

func TestGraphBuilderNestedMapSet(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	expected := createTestMap(3, 4, valGen)
	b := NewGraphBuilder(vs, MapKind)

	ops := []testGraphOp{}

	isNomsCollectionKind := func(kind NomsKind) bool {
		return kind == MapKind || kind == SetKind || kind == ListKind
	}
	var generateOps func(keys []Value, col Value)
	generateOps = func(keys []Value, col Value) {
		switch c := col.(type) {
		case Map:
			c.Iter(func(k, v Value) bool {
				if isNomsCollectionKind(v.Kind()) {
					newKeys := append(keys, k)
					generateOps(newKeys, v)
				} else {
					tgo := testGraphOp{keys: dupSlice(keys), kind: MapKind, item: mapEntry{k, v}}
					ops = append(ops, tgo)
				}
				return false
			})
		case List:
			c.Iter(func(v Value, idx uint64) bool {
				tgo := testGraphOp{keys: dupSlice(keys), kind: ListKind, item: v}
				ops = append(ops, tgo)
				return false
			})
		case Set:
			c.Iter(func(v Value) bool {
				tgo := testGraphOp{keys: dupSlice(keys), kind: SetKind, item: v}
				ops = append(ops, tgo)
				return false
			})
		}
	}
	generateOps(nil, expected)
	shuffle(ops)

	for _, op := range ops {
		switch op.kind {
		case MapKind:
			b.MapSet(op.keys, op.item.(mapEntry).key, op.item.(mapEntry).value)
		case SetKind:
			b.SetInsert(op.keys, op.item.(Value))
		case ListKind:
			b.ListAppend(op.keys, op.item.(Value))
		}
	}

	v := b.Build()
	assert.NotNil(v)
	assert.True(expected.Equals(v))
}

func ExampleGraphBuilder_Build() {
	vs := newTestValueStore()
	defer vs.Close()

	gb := NewGraphBuilder(vs, MapKind)
	gb.SetInsert([]Value{String("parent"), String("children")}, String("John"))
	gb.SetInsert([]Value{String("parent"), String("children")}, String("Mary"))
	gb.SetInsert([]Value{String("parent"), String("children")}, String("Frieda"))
	gb.MapSet([]Value{String("parent"), String("ages")}, String("Father"), Number(42))
	gb.MapSet([]Value{String("parent"), String("ages")}, String("Mother"), Number(44))
	gb.ListAppend([]Value{String("parent"), String("chores")}, String("Make dinner"))
	gb.ListAppend([]Value{String("parent"), String("chores")}, String("Wash dishes"))
	gb.ListAppend([]Value{String("parent"), String("chores")}, String("Make breakfast"))
	gb.ListAppend([]Value{String("parent"), String("chores")}, String("Wash dishes"))
	gb.MapSet([]Value{String("parent")}, String("combinedAge"), Number(86))
	m := gb.Build()
	fmt.Println("map:", EncodedValue(m))
}
