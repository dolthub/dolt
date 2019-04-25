// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const testMapSize = 8000

type genValueFn func(i int) Value

type testMap struct {
	entries     mapEntrySlice
	knownBadKey Value
}

func (tm testMap) SetValue(i int, v Value) testMap {
	entries := make([]mapEntry, 0, len(tm.entries))
	entries = append(entries, tm.entries...)
	entries[i].value = v
	return testMap{entries, tm.knownBadKey}
}

func (tm testMap) Remove(from, to int) testMap {
	entries := make([]mapEntry, 0, len(tm.entries)-(to-from))
	entries = append(entries, tm.entries[:from]...)
	entries = append(entries, tm.entries[to:]...)
	return testMap{entries, tm.knownBadKey}
}

func (tm testMap) MaybeGet(key Value) (v Value, ok bool) {
	for _, entry := range tm.entries {
		if entry.key.Equals(key) {
			return entry.value, true
		}
	}
	return nil, false
}

func (tm testMap) Diff(last testMap) (added []Value, removed []Value, modified []Value) {
	// Note: this could be use tm.toMap/last.toMap and then tmMap.Diff(lastMap) but the
	// purpose of this method is to be redundant.
	if len(tm.entries) == 0 && len(last.entries) == 0 {
		return // nothing changed
	}
	if len(tm.entries) == 0 {
		// everything removed
		for _, entry := range last.entries {
			removed = append(removed, entry.key)
		}
		return
	}
	if len(last.entries) == 0 {
		// everything added
		for _, entry := range tm.entries {
			added = append(added, entry.key)
		}
		return
	}

	for _, entry := range tm.entries {
		otherValue, exists := last.MaybeGet(entry.key)
		if !exists {
			added = append(added, entry.key)
		} else if !entry.value.Equals(otherValue) {
			modified = append(modified, entry.key)
		}
	}
	for _, entry := range last.entries {
		_, exists := tm.MaybeGet(entry.key)
		if !exists {
			removed = append(removed, entry.key)
		}
	}
	return
}

func (tm testMap) toMap(vrw ValueReadWriter) Map {
	keyvals := []Value{}
	for _, entry := range tm.entries {
		keyvals = append(keyvals, entry.key, entry.value)
	}
	return NewMap(vrw, keyvals...)
}

func toValuable(vs ValueSlice) []Valuable {
	vb := make([]Valuable, len(vs))
	for i, v := range vs {
		vb[i] = v
	}
	return vb
}

func (tm testMap) Flatten(from, to int) []Value {
	flat := make([]Value, 0, len(tm.entries)*2)
	for _, entry := range tm.entries[from:to] {
		flat = append(flat, entry.key)
		flat = append(flat, entry.value)
	}
	return flat
}

func (tm testMap) FlattenAll() []Value {
	return tm.Flatten(0, len(tm.entries))
}

func newSortedTestMap(length int, gen genValueFn) testMap {
	keys := make(ValueSlice, 0, length)
	for i := 0; i < length; i++ {
		keys = append(keys, gen(i))
	}

	sort.Sort(keys)

	entries := make([]mapEntry, 0, len(keys))
	for i, k := range keys {
		entries = append(entries, mapEntry{k, Float(i * 2)})
	}

	return testMap{entries, Float(length + 2)}
}

func newTestMapFromMap(m Map) testMap {
	entries := make([]mapEntry, 0, m.Len())
	m.IterAll(func(key, value Value) {
		entries = append(entries, mapEntry{key, value})
	})
	return testMap{entries, Float(-0)}
}

func newRandomTestMap(length int, gen genValueFn) testMap {
	s := rand.NewSource(4242)
	used := map[int]bool{}

	mask := int(0xffffff)
	entries := make([]mapEntry, 0, length)
	for len(entries) < length {
		v := int(s.Int63()) & mask
		if _, ok := used[v]; !ok {
			entry := mapEntry{gen(v), gen(v * 2)}
			entries = append(entries, entry)
			used[v] = true
		}
	}

	return testMap{entries, gen(mask + 1)}
}

func validateMap(t *testing.T, vrw ValueReadWriter, m Map, entries mapEntrySlice) {
	tm := testMap{entries: entries}
	assert.True(t, m.Equals(tm.toMap(vrw)))

	out := mapEntrySlice{}
	m.IterAll(func(k Value, v Value) {
		out = append(out, mapEntry{k, v})
	})

	assert.True(t, out.Equals(entries))
}

type mapTestSuite struct {
	collectionTestSuite
	elems testMap
}

func newMapTestSuite(size uint, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int, gen genValueFn) *mapTestSuite {
	vrw := newTestValueStore()

	length := 1 << size
	keyType := TypeOf(gen(0))
	elems := newSortedTestMap(length, gen)
	tr := MakeMapType(keyType, FloaTType)
	tmap := NewMap(vrw, elems.FlattenAll()...)
	return &mapTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    tmap,
			expectType:             tr,
			expectLen:              uint64(length),
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				if v2.Len() != uint64(elems.entries.Len()) {
					fmt.Println("lengths not equal:", v2.Len(), elems.entries.Len())
					return false
				}
				l2 := v2.(Map)
				idx := uint64(0)
				l2.Iter(func(key, value Value) (stop bool) {
					entry := elems.entries[idx]
					if !key.Equals(entry.key) {
						fmt.Printf("%d: %s (%s)\n!=\n%s (%s)\n", idx, EncodedValue(key), key.Hash(), EncodedValue(entry.key), entry.key.Hash())
						stop = true
					}
					if !value.Equals(entry.value) {
						fmt.Printf("%s (%s) !=\n%s (%s)\n", EncodedValue(value), value.Hash(), EncodedValue(entry.value), entry.value.Hash())
						stop = true
					}
					idx++
					return
				})
				return idx == v2.Len()
			},
			prependOne: func() Collection {
				dup := make([]mapEntry, length+1)
				dup[0] = mapEntry{Float(-1), Float(-2)}
				copy(dup[1:], elems.entries)
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(vrw, flat...)
			},
			appendOne: func() Collection {
				dup := make([]mapEntry, length+1)
				copy(dup, elems.entries)
				dup[len(dup)-1] = mapEntry{Float(length*2 + 1), Float((length*2 + 1) * 2)}
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(vrw, flat...)
			},
		},
		elems: elems,
	}
}

func (suite *mapTestSuite) createStreamingMap(vs *ValueStore) Map {
	kvChan := make(chan Value)
	mapChan := NewStreamingMap(vs, kvChan)
	for _, entry := range suite.elems.entries {
		kvChan <- entry.key
		kvChan <- entry.value
	}
	close(kvChan)
	return <-mapChan
}

func (suite *mapTestSuite) TestStreamingMap() {
	vs := newTestValueStore()
	defer vs.Close()
	m := suite.createStreamingMap(vs)
	suite.True(suite.validate(m), "map not valid")
}

func (suite *mapTestSuite) TestStreamingMapOrder() {
	vs := newTestValueStore()
	defer vs.Close()

	entries := make(mapEntrySlice, len(suite.elems.entries))
	copy(entries, suite.elems.entries)
	entries[0], entries[1] = entries[1], entries[0]

	kvChan := make(chan Value, len(entries)*2)
	for _, e := range entries {
		kvChan <- e.key
		kvChan <- e.value
	}
	close(kvChan)

	readInput := func(vrw ValueReadWriter, kvs <-chan Value, outChan chan<- Map) {
		readMapInput(vrw, kvs, outChan)
	}

	testFunc := func() {
		outChan := newStreamingMap(vs, kvChan, readInput)
		<-outChan
	}

	suite.Panics(testFunc)
}

func (suite *mapTestSuite) TestStreamingMap2() {
	wg := sync.WaitGroup{}
	vs := newTestValueStore()
	defer vs.Close()

	wg.Add(2)
	var m1, m2 Map
	go func() {
		m1 = suite.createStreamingMap(vs)
		wg.Done()
	}()
	go func() {
		m2 = suite.createStreamingMap(vs)
		wg.Done()
	}()
	wg.Wait()
	suite.True(suite.validate(m1), "map 'm1' not valid")
	suite.True(suite.validate(m2), "map 'm2' not valid")
}

func TestMapSuite4K(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, 4, 2, 2, newNumber))
}

func TestMapSuite4KStructs(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, 11, 2, 2, newNumberStruct))
}

func newNumber(i int) Value {
	return Float(i)
}

func newNumberStruct(i int) Value {
	return NewStruct("", StructData{"n": Float(i)})
}

func getTestNativeOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, newNumber)
}

func getTestRefValueOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, newNumber)
}

func getTestRefToNativeOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, func(i int) Value {
		return vrw.WriteValue(context.Background(), Float(i))
	})
}

func getTestRefToValueOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, func(i int) Value {
		return vrw.WriteValue(context.Background(), NewSet(vrw, Float(i)))
	})
}

func accumulateMapDiffChanges(m1, m2 Map) (added []Value, removed []Value, modified []Value) {
	changes := make(chan ValueChanged)
	go func() {
		m1.Diff(m2, changes, nil)
		close(changes)
	}()
	for change := range changes {
		if change.ChangeType == DiffChangeAdded {
			added = append(added, change.Key)
		} else if change.ChangeType == DiffChangeRemoved {
			removed = append(removed, change.Key)
		} else {
			modified = append(modified, change.Key)
		}
	}
	return
}

func diffMapTest(assert *assert.Assertions, m1 Map, m2 Map, numAddsExpected int, numRemovesExpected int, numModifiedExpected int) (added []Value, removed []Value, modified []Value) {
	added, removed, modified = accumulateMapDiffChanges(m1, m2)
	assert.Equal(numAddsExpected, len(added), "num added is not as expected")
	assert.Equal(numRemovesExpected, len(removed), "num removed is not as expected")
	assert.Equal(numModifiedExpected, len(modified), "num modified is not as expected")

	tm1 := newTestMapFromMap(m1)
	tm2 := newTestMapFromMap(m2)
	tmAdded, tmRemoved, tmModified := tm1.Diff(tm2)
	assert.Equal(numAddsExpected, len(tmAdded), "num added is not as expected")
	assert.Equal(numRemovesExpected, len(tmRemoved), "num removed is not as expected")
	assert.Equal(numModifiedExpected, len(tmModified), "num modified is not as expected")

	assert.Equal(added, tmAdded, "map added != tmMap added")
	assert.Equal(removed, tmRemoved, "map removed != tmMap removed")
	assert.Equal(modified, tmModified, "map modified != tmMap modified")
	return
}

func TestMapDiff(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	testMap1 := newRandomTestMap(64*2, newNumber)
	testMap2 := newRandomTestMap(64*2, newNumber)
	testMapAdded, testMapRemoved, testMapModified := testMap1.Diff(testMap2)
	map1 := testMap1.toMap(vrw)
	map2 := testMap2.toMap(vrw)

	mapDiffAdded, mapDiffRemoved, mapDiffModified := accumulateMapDiffChanges(map1, map2)
	assert.Equal(t, testMapAdded, mapDiffAdded, "testMap.diff != map.diff")
	assert.Equal(t, testMapRemoved, mapDiffRemoved, "testMap.diff != map.diff")
	assert.Equal(t, testMapModified, mapDiffModified, "testMap.diff != map.diff")
}

func TestMapMutationReadWriteCount(t *testing.T) {
	// This test is a sanity check that we are reading a "reasonable" number of
	// sequences while mutating maps.
	// TODO: We are currently un-reasonable.
	temp := MakeStructTemplate("Foo", []string{"Bool", "Number", "String1", "String2"})

	newLargeStruct := func(i int) Value {
		return temp.NewStruct([]Value{
			Bool(i%2 == 0),
			Float(i),
			String(fmt.Sprintf("I AM A REALLY REALY REALL SUPER CALIFRAGILISTICLY CRAZY-ASSED LONGTASTIC String %d", i)),
			String(fmt.Sprintf("I am a bit shorter and also more chill: %d", i)),
		})
	}

	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := newValueStoreWithCacheAndPending(cs, 0, 0)

	me := NewMap(vs).Edit()
	for i := 0; i < 10000; i++ {
		me.Set(Float(i), newLargeStruct(i))
	}
	m := me.Map(context.Background())
	r := vs.WriteValue(context.Background(), m)
	vs.Commit(context.Background(), vs.Root(context.Background()), vs.Root(context.Background()))
	m = r.TargetValue(context.Background(), vs).(Map)

	every := 100

	me = m.Edit()
	for i := 0; i < 10000; i++ {
		if i%every == 0 {
			k := Float(i)
			s := me.Get(Float(i)).(Struct)
			s = s.Set("Number", Float(float64(s.Get("Number").(Float))+1))
			me.Set(k, s)
		}
		i++
	}

	cs.Writes = 0
	cs.Reads = 0

	m = me.Map(context.Background())

	vs.Commit(context.Background(), vs.Root(context.Background()), vs.Root(context.Background()))

	assert.Equal(t, uint64(3), NewRef(m).Height())
	assert.Equal(t, 105, cs.Reads)
	assert.Equal(t, 62, cs.Writes)
}

func TestMapInfiniteChunkBug(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	keyLen := chunkWindow + 1

	buff := &bytes.Buffer{}
	for i := uint32(0); i < keyLen; i++ {
		buff.WriteString("s")
	}

	prefix := buff.String()

	me := NewMap(vrw).Edit()

	for i := 0; i < 10000; i++ {
		me.Set(String(prefix+fmt.Sprintf("%d", i)), Float(i))
	}

	me.Map(context.Background())
}

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m := NewMap(vrw)
	assert.Equal(uint64(0), m.Len())
	m = NewMap(vrw, String("foo1"), String("bar1"), String("foo2"), String("bar2"))
	assert.Equal(uint64(2), m.Len())
	assert.True(String("bar1").Equals(m.Get(String("foo1"))))
	assert.True(String("bar2").Equals(m.Get(String("foo2"))))
}

func TestMapUniqueKeysString(t *testing.T) {
	vrw := newTestValueStore()

	assert := assert.New(t)
	l := []Value{
		String("hello"), String("world"),
		String("foo"), String("bar"),
		String("bar"), String("foo"),
		String("hello"), String("foo"),
	}
	m := NewMap(vrw, l...)
	assert.Equal(uint64(3), m.Len())
	assert.True(String("foo").Equals(m.Get(String("hello"))))
}

func TestMapUniqueKeysNumber(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	l := []Value{
		Float(4), Float(1),
		Float(0), Float(2),
		Float(1), Float(2),
		Float(3), Float(4),
		Float(1), Float(5),
	}
	m := NewMap(vrw, l...)
	assert.Equal(uint64(4), m.Len())
	assert.True(Float(5).Equals(m.Get(Float(1))))
}

type toTestMapFunc func(scale int, vrw ValueReadWriter) testMap

func TestMapHas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		m2 := vrw.ReadValue(context.Background(), vrw.WriteValue(context.Background(), m).TargetHash()).(Map)
		for _, entry := range tm.entries {
			k, v := entry.key, entry.value
			assert.True(m.Has(k))
			assert.True(m.Get(k).Equals(v))
			assert.True(m2.Has(k))
			assert.True(m2.Get(k).Equals(v))
		}
		diffMapTest(assert, m, m2, 0, 0, 0)
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapHasRemove(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	me := NewMap(vrw).Edit()
	bothHave := func(k Value) bool {
		meHas := me.Has(k)
		mHas := me.Map(context.Background()).Has(k)
		assert.Equal(meHas, mHas)
		return meHas
	}

	assert.False(bothHave(String("a")))

	me.Set(String("a"), String("a"))
	assert.True(bothHave(String("a")))

	me.Remove(String("a"))
	assert.False(bothHave(String("a")))

	me.Set(String("a"), String("a"))
	assert.True(bothHave(String("a")))

	me.Set(String("a"), String("a"))
	assert.True(bothHave(String("a")))

	// In-order insertions
	me.Set(String("b"), String("b"))
	me.Set(String("c"), String("c"))
	assert.True(bothHave(String("a")))
	assert.True(bothHave(String("b")))
	assert.True(bothHave(String("c")))

	// Out-of-order insertions
	me.Set(String("z"), String("z"))
	me.Set(String("y"), String("y"))
	assert.True(bothHave(String("z")))
	assert.True(bothHave(String("y")))
	assert.True(bothHave(String("a")))
	assert.True(bothHave(String("b")))
	assert.True(bothHave(String("c")))

	// Removals
	me.Remove(String("z")).Remove(String("y")).Remove(String("a")).Remove(String("b")).Remove(String("c")).Remove(String("never-inserted"))
	assert.False(bothHave(String("z")))
	assert.False(bothHave(String("y")))
	assert.False(bothHave(String("a")))
	assert.False(bothHave(String("b")))
	assert.False(bothHave(String("c")))

	m := me.Map(context.Background())
	assert.True(m.Len() == 0)
}

func TestMapRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(incr int, toTestMap toTestMapFunc, scale int) {
		vs := newTestValueStore()
		tm := toTestMap(scale, vs)
		whole := tm.toMap(vs)
		run := func(i int) {
			expected := tm.Remove(i, i+1).toMap(vs)
			actual := whole.Edit().Remove(tm.entries[i].key).Map(context.Background())
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries); i += incr {
			run(i)
		}
		run(len(tm.entries) - 1)
	}

	doTest(128, getTestNativeOrderMap, 32)
	doTest(64, getTestRefValueOrderMap, 4)
	doTest(64, getTestRefToNativeOrderMap, 4)
	doTest(64, getTestRefToValueOrderMap, 4)
}

func TestMapRemoveNonexistentKey(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	tm := getTestNativeOrderMap(2, vrw)
	original := tm.toMap(vrw)
	actual := original.Edit().Remove(Float(-1)).Map(context.Background()) // rand.Int63 returns non-negative numbers.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestMapFirst(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	m1 := NewMap(vrw)
	k, v := m1.First()
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(context.Background())
	ak, av := m1.First()
	var ek, ev Value

	m1.Iter(func(k, v Value) (stop bool) {
		ek, ev = k, v
		return true
	})

	assert.True(ek.Equals(ak))
	assert.True(ev.Equals(av))
}

func TestMapFirst2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		sort.Stable(tm.entries)
		actualKey, actualValue := m.First()
		assert.True(tm.entries[0].key.Equals(actualKey))
		assert.True(tm.entries[0].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapLast(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	m1 := NewMap(vrw)
	k, v := m1.First()
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(context.Background())
	ak, av := m1.Last()
	var ek, ev Value

	m1.Iter(func(k, v Value) (stop bool) {
		ek, ev = k, v
		return false
	})

	assert.True(ek.Equals(ak))
	assert.True(ev.Equals(av))
}

func TestMapLast2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		sort.Stable(tm.entries)
		actualKey, actualValue := m.Last()
		assert.True(tm.entries[len(tm.entries)-1].key.Equals(actualKey))
		assert.True(tm.entries[len(tm.entries)-1].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapSetGet(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	me := NewMap(vrw).Edit()
	bothAre := func(k Value) Value {
		meV := me.Get(k)
		mV := me.Map(context.Background()).Get(k)
		assert.True((meV == nil && mV == nil) || meV.(Value).Equals(mV))
		return mV
	}

	assert.Nil(bothAre(String("a")))

	me.Set(String("a"), Float(42))
	assert.True(Float(42).Equals(bothAre(String("a"))))

	me.Set(String("a"), Float(43))
	assert.True(Float(43).Equals(bothAre(String("a"))))

	me.Remove(String("a"))
	assert.Nil(bothAre(String("a")))

	// in-order insertions
	me.Set(String("b"), Float(43))
	me.Set(String("c"), Float(44))

	assert.True(Float(43).Equals(bothAre(String("b"))))
	assert.True(Float(44).Equals(bothAre(String("c"))))

	// out-of-order insertions
	me.Set(String("z"), Float(0))
	me.Set(String("y"), Float(1))

	assert.True(Float(0).Equals(bothAre(String("z"))))
	assert.True(Float(1).Equals(bothAre(String("y"))))

	// removals
	me.Remove(String("z"))
	me.Remove(String("a"))
	me.Remove(String("y"))
	me.Remove(String("b"))
	me.Remove(String("c"))

	assert.Nil(bothAre(String("a")))
	assert.Nil(bothAre(String("b")))
	assert.Nil(bothAre(String("c")))
	assert.Nil(bothAre(String("y")))
	assert.Nil(bothAre(String("z")))
	assert.Nil(bothAre(String("never-inserted")))

	m := me.Map(context.Background())
	assert.True(m.Len() == 0)
}

func validateMapInsertion(t *testing.T, tm testMap) {
	vrw := newTestValueStore()

	allMe := NewMap(vrw).Edit()
	incrMe := NewMap(vrw).Edit()

	for i, entry := range tm.entries {
		allMe.Set(entry.key, entry.value)
		incrMe.Set(entry.key, entry.value)

		m1 := allMe.Map(context.Background())
		m2 := incrMe.Map(context.Background())

		validateMap(t, vrw, m1, tm.entries[0:i+1])
		validateMap(t, vrw, m2, tm.entries[0:i+1])

		incrMe = m2.Edit()
	}
}

func TestMapValidateInsertAscending(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	validateMapInsertion(t, newSortedTestMap(300, newNumber))
}

func TestMapSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(incr, offset int, toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		expected := tm.toMap(vrw)
		run := func(from, to int) {
			actual := tm.Remove(from, to).toMap(vrw).Edit().SetM(toValuable(tm.Flatten(from, to))...).Map(context.Background())
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(tm.entries)-offset, len(tm.entries))
	}

	doTest(18, 3, getTestNativeOrderMap, 9)
	doTest(128, 1, getTestNativeOrderMap, 32)
	doTest(64, 1, getTestRefValueOrderMap, 4)
	doTest(64, 1, getTestRefToNativeOrderMap, 4)
	doTest(64, 1, getTestRefToValueOrderMap, 4)
}

func TestMapSetM(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m1 := NewMap(vrw)
	m2 := m1.Edit().SetM().Map(context.Background())
	assert.True(m1.Equals(m2))
	m3 := m2.Edit().SetM(String("foo"), String("bar"), String("hot"), String("dog")).Map(context.Background())
	assert.Equal(uint64(2), m3.Len())
	assert.True(String("bar").Equals(m3.Get(String("foo"))))
	assert.True(String("dog").Equals(m3.Get(String("hot"))))
	m4 := m3.Edit().SetM(String("mon"), String("key")).Map(context.Background())
	assert.Equal(uint64(2), m3.Len())
	assert.Equal(uint64(3), m4.Len())
}

func TestMapSetExistingKeyToNewValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)

	tm := getTestNativeOrderMap(2, vrw)
	original := tm.toMap(vrw)

	expectedWorking := tm
	actual := original
	for i, entry := range tm.entries {
		newValue := Float(int64(entry.value.(Float)) + 1)
		expectedWorking = expectedWorking.SetValue(i, newValue)
		actual = actual.Edit().Set(entry.key, newValue).Map(context.Background())
	}

	expected := expectedWorking.toMap(vrw)
	assert.Equal(expected.Len(), actual.Len())
	assert.True(expected.Equals(actual))
	assert.False(original.Equals(actual))
	diffMapTest(assert, expected, actual, 0, 0, 0)
}

// BUG 98
func TestMapDuplicateSet(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m1 := NewMap(vrw, Bool(true), Bool(true), Float(42), Float(42), Float(42), Float(42))
	assert.Equal(uint64(2), m1.Len())
}

func TestMapMaybeGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		for _, entry := range tm.entries {
			v, ok := m.MaybeGet(entry.key)
			if assert.True(ok, "%v should have been in the map!", entry.key) {
				assert.True(v.Equals(entry.value), "%v != %v", v, entry.value)
			}
		}
		_, ok := m.MaybeGet(tm.knownBadKey)
		assert.False(ok, "m should not contain %v", tm.knownBadKey)
	}

	doTest(getTestNativeOrderMap, 2)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapIter(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m := NewMap(vrw)

	type entry struct {
		key   Value
		value Value
	}

	type resultList []entry
	results := resultList{}
	got := func(key, val Value) bool {
		for _, r := range results {
			if key.Equals(r.key) && val.Equals(r.value) {
				return true
			}
		}
		return false
	}

	stop := false
	cb := func(k, v Value) bool {
		results = append(results, entry{k, v})
		return stop
	}

	m.Iter(cb)
	assert.Equal(0, len(results))

	m = m.Edit().Set(String("a"), Float(0)).Set(String("b"), Float(1)).Map(context.Background())
	m.Iter(cb)
	assert.Equal(2, len(results))
	assert.True(got(String("a"), Float(0)))
	assert.True(got(String("b"), Float(1)))

	results = resultList{}
	stop = true
	m.Iter(cb)
	assert.Equal(1, len(results))
	// Iteration order not guaranteed, but it has to be one of these.
	assert.True(got(String("a"), Float(0)) || got(String("b"), Float(1)))
}

func TestMapIter2(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		sort.Sort(tm.entries)
		idx := uint64(0)
		endAt := uint64(64)

		m.Iter(func(k, v Value) (done bool) {
			assert.True(tm.entries[idx].key.Equals(k))
			assert.True(tm.entries[idx].value.Equals(v))
			if idx == endAt {
				done = true
			}
			idx++
			return
		})

		assert.Equal(endAt, idx-1)
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapAny(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	p := func(k, v Value) bool {
		return k.Equals(String("foo")) && v.Equals(String("bar"))
	}

	assert.False(NewMap(vrw).Any(p))
	assert.False(NewMap(vrw, String("foo"), String("baz")).Any(p))
	assert.True(NewMap(vrw, String("foo"), String("bar")).Any(p))
}

func TestMapIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		sort.Sort(tm.entries)
		idx := uint64(0)

		m.IterAll(func(k, v Value) {
			assert.True(tm.entries[idx].key.Equals(k))
			assert.True(tm.entries[idx].value.Equals(v))
			idx++
		})
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapEquals(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	m1 := NewMap(vrw)
	m2 := m1
	m3 := NewMap(vrw)

	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.True(m3.Equals(m2))
	assert.True(m2.Equals(m3))
	diffMapTest(assert, m1, m2, 0, 0, 0)
	diffMapTest(assert, m1, m3, 0, 0, 0)
	diffMapTest(assert, m2, m1, 0, 0, 0)
	diffMapTest(assert, m2, m3, 0, 0, 0)
	diffMapTest(assert, m3, m1, 0, 0, 0)
	diffMapTest(assert, m3, m2, 0, 0, 0)

	m1 = NewMap(vrw, String("foo"), Float(0.0), String("bar"), NewList(vrw))
	m2 = m2.Edit().Set(String("foo"), Float(0.0)).Set(String("bar"), NewList(vrw)).Map(context.Background())
	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.False(m2.Equals(m3))
	assert.False(m3.Equals(m2))
	diffMapTest(assert, m1, m2, 0, 0, 0)
	diffMapTest(assert, m1, m3, 2, 0, 0)
	diffMapTest(assert, m2, m1, 0, 0, 0)
	diffMapTest(assert, m2, m3, 2, 0, 0)
	diffMapTest(assert, m3, m1, 0, 2, 0)
	diffMapTest(assert, m3, m2, 0, 2, 0)
}

func TestMapNotStringKeys(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	b1 := NewBlob(context.Background(), vrw, bytes.NewBufferString("blob1"))
	b2 := NewBlob(context.Background(), vrw, bytes.NewBufferString("blob2"))
	l := []Value{
		Bool(true), String("true"),
		Bool(false), String("false"),
		Float(1), String("Float: 1"),
		Float(0), String("Float: 0"),
		b1, String("blob1"),
		b2, String("blob2"),
		NewList(vrw), String("empty list"),
		NewList(vrw, NewList(vrw)), String("list of list"),
		NewMap(vrw), String("empty map"),
		NewMap(vrw, NewMap(vrw), NewMap(vrw)), String("map of map/map"),
		NewSet(vrw), String("empty set"),
		NewSet(vrw, NewSet(vrw)), String("map of set/set"),
	}
	m1 := NewMap(vrw, l...)
	assert.Equal(uint64(12), m1.Len())
	for i := 0; i < len(l); i += 2 {
		assert.True(m1.Get(l[i]).Equals(l[i+1]))
	}
	assert.Nil(m1.Get(Float(42)))
}

func testMapOrder(assert *assert.Assertions, vrw ValueReadWriter, keyType, valueType *Type, tuples []Value, expectOrdering []Value) {
	m := NewMap(vrw, tuples...)
	i := 0
	m.IterAll(func(key, value Value) {
		assert.Equal(expectOrdering[i].Hash().String(), key.Hash().String())
		i++
	})
}

func TestMapOrdering(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	testMapOrder(assert, vrw,
		StringType, StringType,
		[]Value{
			String("a"), String("unused"),
			String("z"), String("unused"),
			String("b"), String("unused"),
			String("y"), String("unused"),
			String("c"), String("unused"),
			String("x"), String("unused"),
		},
		[]Value{
			String("a"),
			String("b"),
			String("c"),
			String("x"),
			String("y"),
			String("z"),
		},
	)

	testMapOrder(assert, vrw,
		FloaTType, StringType,
		[]Value{
			Float(0), String("unused"),
			Float(1000), String("unused"),
			Float(1), String("unused"),
			Float(100), String("unused"),
			Float(2), String("unused"),
			Float(10), String("unused"),
		},
		[]Value{
			Float(0),
			Float(1),
			Float(2),
			Float(10),
			Float(100),
			Float(1000),
		},
	)

	testMapOrder(assert, vrw,
		UintType, StringType,
		[]Value{
			Uint(0), String("unused"),
			Uint(1000), String("unused"),
			Uint(1), String("unused"),
			Uint(100), String("unused"),
			Uint(2), String("unused"),
			Uint(10), String("unused"),
		},
		[]Value{
			Uint(0),
			Uint(1),
			Uint(2),
			Uint(10),
			Uint(100),
			Uint(1000),
		},
	)

	testMapOrder(assert, vrw,
		UintType, NullType,
		[]Value{
			Uint(0), NullValue,
			Uint(1000), NullValue,
			Uint(1), NullValue,
			Uint(100), NullValue,
			Uint(2), NullValue,
			Uint(10), NullValue,
		},
		[]Value{
			Uint(0),
			Uint(1),
			Uint(2),
			Uint(10),
			Uint(100),
			Uint(1000),
		},
	)

	testMapOrder(assert, vrw,
		NullType, StringType,
		[]Value{
			NullValue, String("val 1"),
			NullValue, String("val 2"),
			NullValue, String("val 3"),
		},
		[]Value{
			NullValue,
		},
	)

	testMapOrder(assert, vrw,
		IntType, StringType,
		[]Value{
			Int(0), String("unused"),
			Int(1000), String("unused"),
			Int(-1), String("unused"),
			Int(100), String("unused"),
			Int(2), String("unused"),
			Int(-10), String("unused"),
		},
		[]Value{
			Int(-10),
			Int(-1),
			Int(0),
			Int(2),
			Int(100),
			Int(1000),
		},
	)

	testMapOrder(assert, vrw,
		FloaTType, StringType,
		[]Value{
			Float(0), String("unused"),
			Float(-30), String("unused"),
			Float(25), String("unused"),
			Float(1002), NullValue,
			Float(-5050), String("unused"),
			Float(23), String("unused"),
		},
		[]Value{
			Float(-5050),
			Float(-30),
			Float(0),
			Float(23),
			Float(25),
			Float(1002),
		},
	)

	uuids := []UUID{
		UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000"))),
		UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001"))),
		UUID(uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000001"))),
		UUID(uuid.Must(uuid.Parse("10000000-0000-0001-0000-000000000001"))),
		UUID(uuid.Must(uuid.Parse("20000000-0000-0000-0000-000000000001"))),
	}
	testMapOrder(assert, vrw, UUIDType, StringType,
		[]Value{
			uuids[4], String("unused"),
			uuids[1], String("unused"),
			uuids[3], String("unused"),
			uuids[0], String("unused"),
			uuids[2], String("unused"),
		},
		[]Value{
			uuids[0],
			uuids[1],
			uuids[2],
			uuids[3],
			uuids[4],
		},
	)

	testMapOrder(assert, vrw,
		FloaTType, StringType,
		[]Value{
			Float(0.0001), String("unused"),
			Float(0.000001), String("unused"),
			Float(1), String("unused"),
			Float(25.01e3), String("unused"),
			Float(-32.231123e5), String("unused"),
			Float(23), String("unused"),
		},
		[]Value{
			Float(-32.231123e5),
			Float(0.000001),
			Float(0.0001),
			Float(1),
			Float(23),
			Float(25.01e3),
		},
	)

	testMapOrder(assert, vrw,
		ValueType, StringType,
		[]Value{
			String("a"), String("unused"),
			String("z"), String("unused"),
			String("b"), String("unused"),
			String("y"), String("unused"),
			String("c"), String("unused"),
			String("x"), String("unused"),
		},
		[]Value{
			String("a"),
			String("b"),
			String("c"),
			String("x"),
			String("y"),
			String("z"),
		},
	)

	testMapOrder(assert, vrw,
		BoolType, StringType,
		[]Value{
			Bool(true), String("unused"),
			Bool(false), String("unused"),
		},
		[]Value{
			Bool(false),
			Bool(true),
		},
	)
}

func TestMapEmpty(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	me := NewMap(vrw).Edit()
	empty := func() bool {
		return me.Map(context.Background()).Empty()
	}

	assert.True(empty())
	me.Set(Bool(false), String("hi"))
	assert.False(empty())
	me.Set(NewList(vrw), NewMap(vrw))
	assert.False(empty())
}

func TestMapType(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	emptyMapType := MakeMapType(MakeUnionType(), MakeUnionType())
	m := NewMap(vrw)
	assert.True(TypeOf(m).Equals(emptyMapType))

	m2 := m.Edit().Remove(String("B")).Map(context.Background())
	assert.True(emptyMapType.Equals(TypeOf(m2)))

	tr := MakeMapType(StringType, FloaTType)
	m2 = m.Edit().Set(String("A"), Float(1)).Map(context.Background())
	assert.True(tr.Equals(TypeOf(m2)))

	m2 = m.Edit().Set(String("B"), Float(2)).Set(String("C"), Float(2)).Map(context.Background())
	assert.True(tr.Equals(TypeOf(m2)))

	m3 := m2.Edit().Set(String("A"), Bool(true)).Map(context.Background())
	assert.True(MakeMapType(StringType, MakeUnionType(BoolType, FloaTType)).Equals(TypeOf(m3)), TypeOf(m3).Describe())
	m4 := m3.Edit().Set(Bool(true), Float(1)).Map(context.Background())
	assert.True(MakeMapType(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, FloaTType)).Equals(TypeOf(m4)))
}

func TestMapChunks(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	l1 := NewMap(vrw, Float(0), Float(1))
	c1 := getChunks(l1)
	assert.Len(c1, 0)

	l2 := NewMap(vrw, NewRef(Float(0)), Float(1))
	c2 := getChunks(l2)
	assert.Len(c2, 1)

	l3 := NewMap(vrw, Float(0), NewRef(Float(1)))
	c3 := getChunks(l3)
	assert.Len(c3, 1)
}

func TestMapFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	vrw := newTestValueStore()

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		kvs = append(kvs, Float(i), Float(i+1))
	}

	m := NewMap(vrw, kvs...)
	assert.Equal(deriveCollectionHeight(m), getRefHeightOfCollection(m))
}

func TestMapRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)
	vs := newTestValueStore()

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		k := vs.WriteValue(context.Background(), NewStruct("num", StructData{"n": Float(i)}))
		v := vs.WriteValue(context.Background(), NewStruct("num", StructData{"n": Float(i + 1)}))
		assert.NotNil(k)
		assert.NotNil(v)
		kvs = append(kvs, k, v)
	}

	m := NewMap(vs, kvs...)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(deriveCollectionHeight(m)+1, getRefHeightOfCollection(m))
}

func TestMapModifyAfterRead(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	m := getTestNativeOrderMap(2, vs).toMap(vs)
	// Drop chunk values.
	m = vs.ReadValue(context.Background(), vs.WriteValue(context.Background(), m).TargetHash()).(Map)
	// Modify/query. Once upon a time this would crash.
	fst, fstval := m.First()
	m = m.Edit().Remove(fst).Map(context.Background())
	assert.False(m.Has(fst))

	fst2, _ := m.First()
	assert.True(m.Has(fst2))

	m = m.Edit().Set(fst, fstval).Map(context.Background())
	assert.True(m.Has(fst))
}

func TestMapTypeAfterMutations(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	test := func(n int, c interface{}) {
		values := make([]Value, 2*n)
		for i := 0; i < n; i++ {
			values[2*i] = Float(i)
			values[2*i+1] = Float(i)
		}

		m := NewMap(vrw, values...)
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.asSequence())
		assert.True(TypeOf(m).Equals(MakeMapType(FloaTType, FloaTType)))

		m = m.Edit().Set(String("a"), String("a")).Map(context.Background())
		assert.Equal(m.Len(), uint64(n+1))
		assert.IsType(c, m.asSequence())
		assert.True(TypeOf(m).Equals(MakeMapType(MakeUnionType(FloaTType, StringType), MakeUnionType(FloaTType, StringType))))

		m = m.Edit().Remove(String("a")).Map(context.Background())
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.asSequence())
		assert.True(TypeOf(m).Equals(MakeMapType(FloaTType, FloaTType)))
	}

	test(10, mapLeafSequence{})
	test(8000, metaSequence{})
}

func TestCompoundMapWithValuesOfEveryType(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	v := Float(42)
	kvs := []Value{
		// Values
		Bool(true), v,
		Float(0), v,
		String("hello"), v,
		NewBlob(context.Background(), vrw, bytes.NewBufferString("buf")), v,
		NewSet(vrw, Bool(true)), v,
		NewList(vrw, Bool(true)), v,
		NewMap(vrw, Bool(true), Float(0)), v,
		NewStruct("", StructData{"field": Bool(true)}), v,
		// Refs of values
		NewRef(Bool(true)), v,
		NewRef(Float(0)), v,
		NewRef(String("hello")), v,
		NewRef(NewBlob(context.Background(), vrw, bytes.NewBufferString("buf"))), v,
		NewRef(NewSet(vrw, Bool(true))), v,
		NewRef(NewList(vrw, Bool(true))), v,
		NewRef(NewMap(vrw, Bool(true), Float(0))), v,
		NewRef(NewStruct("", StructData{"field": Bool(true)})), v,
	}

	m := NewMap(vrw, kvs...)
	for i := 1; m.asSequence().isLeaf(); i++ {
		k := Float(i)
		kvs = append(kvs, k, v)
		m = m.Edit().Set(k, v).Map(context.Background())
	}

	assert.Equal(len(kvs)/2, int(m.Len()))
	fk, fv := m.First()
	assert.True(bool(fk.(Bool)))
	assert.True(v.Equals(fv))

	for i, keyOrValue := range kvs {
		if i%2 == 0 {
			assert.True(m.Has(keyOrValue))
			assert.True(v.Equals(m.Get(keyOrValue)))
		} else {
			assert.True(v.Equals(keyOrValue))
		}
	}

	for len(kvs) > 0 {
		k := kvs[0]
		kvs = kvs[2:]
		m = m.Edit().Remove(k).Map(context.Background())
		assert.False(m.Has(k))
		assert.Equal(len(kvs)/2, int(m.Len()))
	}
}

func TestMapRemoveLastWhenNotLoaded(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	reload := func(m Map) Map {
		return vs.ReadValue(context.Background(), vs.WriteValue(context.Background(), m).TargetHash()).(Map)
	}

	tm := getTestNativeOrderMap(4, vs)
	nm := tm.toMap(vs)

	for len(tm.entries) > 0 {
		entr := tm.entries
		last := entr[len(entr)-1]
		entr = entr[:len(entr)-1]
		tm.entries = entr
		nm = reload(nm.Edit().Remove(last.key).Map(context.Background()))
		assert.True(tm.toMap(vs).Equals(nm))
	}
}

func TestMapIterFrom(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	test := func(m Map, start, end Value) ValueSlice {
		res := ValueSlice{}
		m.IterFrom(start, func(k, v Value) bool {
			if end.Less(k) {
				return true
			}
			res = append(res, k, v)
			return false
		})
		return res
	}

	kvs := generateNumbersAsValuesFromToBy(-50, 50, 1)
	m1 := NewMap(vrw, kvs...)
	assert.True(kvs.Equals(test(m1, nil, Float(1000))))
	assert.True(kvs.Equals(test(m1, Float(-1000), Float(1000))))
	assert.True(kvs.Equals(test(m1, Float(-50), Float(1000))))
	assert.True(kvs[2:].Equals(test(m1, Float(-49), Float(1000))))
	assert.True(kvs[2:].Equals(test(m1, Float(-48), Float(1000))))
	assert.True(kvs[4:].Equals(test(m1, Float(-47), Float(1000))))
	assert.True(kvs[98:].Equals(test(m1, Float(48), Float(1000))))
	assert.True(kvs[0:0].Equals(test(m1, Float(100), Float(1000))))
	assert.True(kvs[50:60].Equals(test(m1, Float(0), Float(8))))
}

func TestMapAt(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	values := []Value{Bool(false), Float(42), String("a"), String("b"), String("c"), String("d")}
	m := NewMap(vrw, values...)

	for i := 0; i < len(values); i += 2 {
		k, v := m.At(uint64(i / 2))
		assert.Equal(values[i], k)
		assert.Equal(values[i+1], v)
	}

	assert.Panics(func() {
		m.At(42)
	})
}

func TestMapWithStructShouldHaveOptionalFields(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	list := NewMap(vrw,
		String("one"),
		NewStruct("Foo", StructData{
			"a": Float(1),
		}),
		String("two"),
		NewStruct("Foo", StructData{
			"a": Float(2),
			"b": String("bar"),
		}),
	)
	assert.True(
		MakeMapType(StringType,
			MakeStructType("Foo",
				StructField{"a", FloaTType, false},
				StructField{"b", StringType, true},
			),
		).Equals(TypeOf(list)))

	// transpose
	list = NewMap(vrw,
		NewStruct("Foo", StructData{
			"a": Float(1),
		}),
		String("one"),
		NewStruct("Foo", StructData{
			"a": Float(2),
			"b": String("bar"),
		}),
		String("two"),
	)
	assert.True(
		MakeMapType(
			MakeStructType("Foo",
				StructField{"a", FloaTType, false},
				StructField{"b", StringType, true},
			),
			StringType,
		).Equals(TypeOf(list)))

}

func TestMapWithNil(t *testing.T) {
	vrw := newTestValueStore()

	assert.Panics(t, func() {
		NewMap(nil, Float(42))
	})
	assert.Panics(t, func() {
		NewSet(vrw, Float(42), nil)
	})
	assert.Panics(t, func() {
		NewMap(vrw, String("a"), String("b"), nil, Float(42))
	})
	assert.Panics(t, func() {
		NewSet(vrw, String("a"), String("b"), Float(42), nil)
	})
}

func TestNestedEditing(t *testing.T) {
	vrw := newTestValueStore()

	me0 := NewMap(vrw).Edit()

	// m.a.a
	me1a := NewMap(vrw).Edit()
	me0.Set(String("a"), me1a)
	se2a := NewSet(vrw).Edit()
	me1a.Set(String("a"), se2a)
	se2a.Insert(String("a"))

	// m.b.b
	me1b := NewMap(vrw).Edit()
	me0.Set(String("b"), me1b)
	se2b := NewSet(vrw).Edit()
	me1b.Set(String("b"), se2b)
	se2b.Insert(String("b"))

	mOut := me0.Map(context.Background())
	assert.True(t, mOut.Equals(NewMap(vrw,
		String("a"), NewMap(vrw,
			String("a"), NewSet(vrw, String("a")),
		),
		String("b"), NewMap(vrw,
			String("b"), NewSet(vrw, String("b")),
		),
	)))

	se2a.Remove(String("a")).Insert(String("aa"))
	se2b.Remove(String("b")).Insert(String("bb"))

	mOut = me0.Map(context.Background())
	assert.True(t, mOut.Equals(NewMap(vrw,
		String("a"), NewMap(vrw,
			String("a"), NewSet(vrw, String("aa")),
		),
		String("b"), NewMap(vrw,
			String("b"), NewSet(vrw, String("bb")),
		),
	)))

	se2a.Remove(String("aa"))
	se2b.Remove(String("bb"))

	mOut = me0.Map(context.Background())
	assert.True(t, mOut.Equals(NewMap(vrw))) // remove empty
}
