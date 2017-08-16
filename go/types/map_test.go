// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
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

func (tm testMap) toMap() Map {
	keyvals := []Value{}
	for _, entry := range tm.entries {
		keyvals = append(keyvals, entry.key, entry.value)
	}
	return NewMap(keyvals...)
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
		entries = append(entries, mapEntry{k, Number(i * 2)})
	}

	return testMap{entries, Number(length + 2)}
}

func newTestMapFromMap(m Map) testMap {
	entries := make([]mapEntry, 0, m.Len())
	m.IterAll(func(key, value Value) {
		entries = append(entries, mapEntry{key, value})
	})
	return testMap{entries, Number(-0)}
}

func newRandomTestMap(length int, gen genValueFn) testMap {
	s := rand.NewSource(4242)
	used := map[int]bool{}

	var mask int = 0xffffff
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

func validateMap(t *testing.T, m Map, entries mapEntrySlice) {
	tm := testMap{entries: entries}
	assert.True(t, m.Equals(tm.toMap()))

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
	length := 1 << size
	keyType := TypeOf(gen(0))
	elems := newSortedTestMap(length, gen)
	tr := MakeMapType(keyType, NumberType)
	tmap := NewMap(elems.FlattenAll()...)
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
						fmt.Printf("%d: %s (%s)\n!=\n%s (%s)\n", idx, EncodedValueWithTags(key), key.Hash(), EncodedValueWithTags(entry.key), entry.key.Hash())
						stop = true
					}
					if !value.Equals(entry.value) {
						fmt.Printf("%s (%s) !=\n%s (%s)\n", EncodedValueWithTags(value), value.Hash(), EncodedValueWithTags(entry.value), entry.value.Hash())
						stop = true
					}
					idx++
					return
				})
				return idx == v2.Len()
			},
			prependOne: func() Collection {
				dup := make([]mapEntry, length+1)
				dup[0] = mapEntry{Number(-1), Number(-2)}
				copy(dup[1:], elems.entries)
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(flat...)
			},
			appendOne: func() Collection {
				dup := make([]mapEntry, length+1)
				copy(dup, elems.entries)
				dup[len(dup)-1] = mapEntry{Number(length*2 + 1), Number((length*2 + 1) * 2)}
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(flat...)
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
	suite.Run(t, newMapTestSuite(12, 9, 2, 2, newNumber))
}

func TestMapSuite4KStructs(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, 13, 2, 2, newNumberStruct))
}

func newNumber(i int) Value {
	return Number(i)
}

func newNumberStruct(i int) Value {
	return NewStruct("", StructData{"n": Number(i)})
}

func getTestNativeOrderMap(scale int) testMap {
	return newRandomTestMap(64*scale, newNumber)
}

func getTestRefValueOrderMap(scale int) testMap {
	return newRandomTestMap(64*scale, newNumber)
}

func getTestRefToNativeOrderMap(scale int, vw ValueWriter) testMap {
	return newRandomTestMap(64*scale, func(i int) Value {
		return vw.WriteValue(Number(i))
	})
}

func getTestRefToValueOrderMap(scale int, vw ValueWriter) testMap {
	return newRandomTestMap(64*scale, func(i int) Value {
		return vw.WriteValue(NewSet(Number(i)))
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

	testMap1 := newRandomTestMap(64*2, newNumber)
	testMap2 := newRandomTestMap(64*2, newNumber)
	testMapAdded, testMapRemoved, testMapModified := testMap1.Diff(testMap2)
	map1 := testMap1.toMap()
	map2 := testMap2.toMap()

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
			Number(i),
			String(fmt.Sprintf("I AM A REALLY REALY REALL SUPER CALIFRAGILISTICLY CRAZY-ASSED LONGTASTIC String %d", i)),
			String(fmt.Sprintf("I am a bit shorted and also more chill: %d", i)),
		})
	}

	m := newRandomTestMap(4000, newLargeStruct).toMap()
	every := 100

	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := newValueStoreWithCacheAndPending(cs, 0, 0)
	r := vs.WriteValue(m)
	vs.Commit(vs.Root(), vs.Root())

	cs.Writes = 0
	cs.Reads = 0

	i := 0

	me := vs.ReadValue(r.TargetHash()).(Map).Edit()
	m.IterAll(func(k, v Value) {
		if i%every == 0 {
			s := v.(Struct)

			s = s.Set("Number", Number(float64(s.Get("Number").(Number))+1))
			me.Set(k, s)
		}
		i++
	})

	me.Map(vs)
	vs.Commit(vs.Root(), vs.Root())

	assert.Equal(t, uint64(3), NewRef(m).Height())
	assert.Equal(t, 84, cs.Reads)
	assert.Equal(t, 45, cs.Writes)
}

func TestMapInfiniteChunkBug(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	keyLen := chunkWindow + 1

	buff := &bytes.Buffer{}
	for i := uint32(0); i < keyLen; i++ {
		buff.WriteString("s")
	}

	prefix := buff.String()

	me := NewMap().Edit()

	for i := 0; i < 10000; i++ {
		me.Set(String(prefix+fmt.Sprintf("%d", i)), Number(i))
	}

	me.Map(nil)
}

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()
	assert.Equal(uint64(0), m.Len())
	m = NewMap(String("foo1"), String("bar1"), String("foo2"), String("bar2"))
	assert.Equal(uint64(2), m.Len())
	assert.True(String("bar1").Equals(m.Get(String("foo1"))))
	assert.True(String("bar2").Equals(m.Get(String("foo2"))))
}

func TestMapUniqueKeysString(t *testing.T) {
	assert := assert.New(t)
	l := []Value{
		String("hello"), String("world"),
		String("foo"), String("bar"),
		String("bar"), String("foo"),
		String("hello"), String("foo"),
	}
	m := NewMap(l...)
	assert.Equal(uint64(3), m.Len())
	assert.True(String("foo").Equals(m.Get(String("hello"))))
}

func TestMapUniqueKeysNumber(t *testing.T) {
	assert := assert.New(t)
	l := []Value{
		Number(4), Number(1),
		Number(0), Number(2),
		Number(1), Number(2),
		Number(3), Number(4),
		Number(1), Number(5),
	}
	m := NewMap(l...)
	assert.Equal(uint64(4), m.Len())
	assert.True(Number(5).Equals(m.Get(Number(1))))
}

func TestMapHas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	vs := newTestValueStore()
	doTest := func(tm testMap) {
		m := tm.toMap()
		m2 := vs.ReadValue(vs.WriteValue(m).TargetHash()).(Map)
		for _, entry := range tm.entries {
			k, v := entry.key, entry.value
			assert.True(m.Has(k))
			assert.True(m.Get(k).Equals(v))
			assert.True(m2.Has(k))
			assert.True(m2.Get(k).Equals(v))
		}
		diffMapTest(assert, m, m2, 0, 0, 0)
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, vs))
	doTest(getTestRefToValueOrderMap(2, vs))
}

func TestMapHasRemove(t *testing.T) {
	assert := assert.New(t)

	me := NewMap().Edit()
	bothHave := func(k Value) bool {
		meHas := me.Has(k)
		mHas := me.Map(nil).Has(k)
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

	m := me.Map(nil)
	assert.True(m.Len() == 0)
}

func TestMapRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(incr int, tm testMap) {
		whole := tm.toMap()
		run := func(i int) {
			expected := tm.Remove(i, i+1).toMap()
			actual := whole.Edit().Remove(tm.entries[i].key).Map(nil)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries); i += incr {
			run(i)
		}
		run(len(tm.entries) - 1)
	}

	doTest(128, getTestNativeOrderMap(32))
	doTest(64, getTestRefValueOrderMap(4))
	doTest(64, getTestRefToNativeOrderMap(4, newTestValueStore()))
	doTest(64, getTestRefToValueOrderMap(4, newTestValueStore()))
}

func TestMapRemoveNonexistentKey(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	tm := getTestNativeOrderMap(2)
	original := tm.toMap()
	actual := original.Edit().Remove(Number(-1)).Map(nil) // rand.Int63 returns non-negative numbers.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestMapFirst(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	m1 := NewMap()
	k, v := m1.First()
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(nil)
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

	doTest := func(tm testMap) {
		m := tm.toMap()
		sort.Stable(tm.entries)
		actualKey, actualValue := m.First()
		assert.True(tm.entries[0].key.Equals(actualKey))
		assert.True(tm.entries[0].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, newTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, newTestValueStore()))
}

func TestMapLast(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	m1 := NewMap()
	k, v := m1.First()
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(nil)
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

	doTest := func(tm testMap) {
		m := tm.toMap()
		sort.Stable(tm.entries)
		actualKey, actualValue := m.Last()
		assert.True(tm.entries[len(tm.entries)-1].key.Equals(actualKey))
		assert.True(tm.entries[len(tm.entries)-1].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, newTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, newTestValueStore()))
}

func TestMapSetGet(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	me := NewMap().Edit()
	bothAre := func(k Value) Value {
		meV := me.Get(k)
		mV := me.Map(nil).Get(k)
		assert.True((meV == nil && mV == nil) || meV.(Value).Equals(mV))
		return mV
	}

	assert.Nil(bothAre(String("a")))

	me.Set(String("a"), Number(42))
	assert.True(Number(42).Equals(bothAre(String("a"))))

	me.Set(String("a"), Number(43))
	assert.True(Number(43).Equals(bothAre(String("a"))))

	me.Remove(String("a"))
	assert.Nil(bothAre(String("a")))

	// in-order insertions
	me.Set(String("b"), Number(43))
	me.Set(String("c"), Number(44))

	assert.True(Number(43).Equals(bothAre(String("b"))))
	assert.True(Number(44).Equals(bothAre(String("c"))))

	// out-of-order insertions
	me.Set(String("z"), Number(0))
	me.Set(String("y"), Number(1))

	assert.True(Number(0).Equals(bothAre(String("z"))))
	assert.True(Number(1).Equals(bothAre(String("y"))))

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

	m := me.Map(nil)
	assert.True(m.Len() == 0)
}

func validateMapInsertion(t *testing.T, tm testMap) {
	allMe := NewMap().Edit()
	incrMe := NewMap().Edit()

	for i, entry := range tm.entries {
		allMe.Set(entry.key, entry.value)
		incrMe.Set(entry.key, entry.value)

		m1 := allMe.Map(nil)
		m2 := incrMe.Map(nil)

		validateMap(t, m1, tm.entries[0:i+1])
		validateMap(t, m2, tm.entries[0:i+1])

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

	doTest := func(incr, offset int, tm testMap) {
		expected := tm.toMap()
		run := func(from, to int) {
			actual := tm.Remove(from, to).toMap().Edit().SetM(toValuable(tm.Flatten(from, to))...).Map(nil)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(tm.entries)-offset, len(tm.entries))
	}

	doTest(18, 3, getTestNativeOrderMap(9))
	doTest(128, 1, getTestNativeOrderMap(32))
	doTest(64, 1, getTestRefValueOrderMap(4))
	doTest(64, 1, getTestRefToNativeOrderMap(4, newTestValueStore()))
	doTest(64, 1, getTestRefToValueOrderMap(4, newTestValueStore()))
}

func TestMapSetM(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	m2 := m1.Edit().SetM().Map(nil)
	assert.True(m1.Equals(m2))
	m3 := m2.Edit().SetM(String("foo"), String("bar"), String("hot"), String("dog")).Map(nil)
	assert.Equal(uint64(2), m3.Len())
	assert.True(String("bar").Equals(m3.Get(String("foo"))))
	assert.True(String("dog").Equals(m3.Get(String("hot"))))
	m4 := m3.Edit().SetM(String("mon"), String("key")).Map(nil)
	assert.Equal(uint64(2), m3.Len())
	assert.Equal(uint64(3), m4.Len())
}

func TestMapSetExistingKeyToNewValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	tm := getTestNativeOrderMap(2)
	original := tm.toMap()

	expectedWorking := tm
	actual := original
	for i, entry := range tm.entries {
		newValue := Number(int64(entry.value.(Number)) + 1)
		expectedWorking = expectedWorking.SetValue(i, newValue)
		actual = actual.Edit().Set(entry.key, newValue).Map(nil)
	}

	expected := expectedWorking.toMap()
	assert.Equal(expected.Len(), actual.Len())
	assert.True(expected.Equals(actual))
	assert.False(original.Equals(actual))
	diffMapTest(assert, expected, actual, 0, 0, 0)
}

// BUG 98
func TestMapDuplicateSet(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap(Bool(true), Bool(true), Number(42), Number(42), Number(42), Number(42))
	assert.Equal(uint64(2), m1.Len())
}

func TestMapMaybeGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toMap()
		for _, entry := range tm.entries {
			v, ok := m.MaybeGet(entry.key)
			if assert.True(ok, "%v should have been in the map!", entry.key) {
				assert.True(v.Equals(entry.value), "%v != %v", v, entry.value)
			}
		}
		_, ok := m.MaybeGet(tm.knownBadKey)
		assert.False(ok, "m should not contain %v", tm.knownBadKey)
	}

	doTest(getTestNativeOrderMap(2))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, newTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, newTestValueStore()))
}

func TestMapIter(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()

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

	m = m.Edit().Set(String("a"), Number(0)).Set(String("b"), Number(1)).Map(nil)
	m.Iter(cb)
	assert.Equal(2, len(results))
	assert.True(got(String("a"), Number(0)))
	assert.True(got(String("b"), Number(1)))

	results = resultList{}
	stop = true
	m.Iter(cb)
	assert.Equal(1, len(results))
	// Iteration order not guaranteed, but it has to be one of these.
	assert.True(got(String("a"), Number(0)) || got(String("b"), Number(1)))
}

func TestMapIter2(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(tm testMap) {
		m := tm.toMap()
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

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, newTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, newTestValueStore()))
}

func TestMapAny(t *testing.T) {
	assert := assert.New(t)

	p := func(k, v Value) bool {
		return k.Equals(String("foo")) && v.Equals(String("bar"))
	}

	assert.False(NewMap().Any(p))
	assert.False(NewMap(String("foo"), String("baz")).Any(p))
	assert.True(NewMap(String("foo"), String("bar")).Any(p))
}

func TestMapIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toMap()
		sort.Sort(tm.entries)
		idx := uint64(0)

		m.IterAll(func(k, v Value) {
			assert.True(tm.entries[idx].key.Equals(k))
			assert.True(tm.entries[idx].value.Equals(v))
			idx++
		})
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, newTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, newTestValueStore()))
}

func TestMapEquals(t *testing.T) {
	assert := assert.New(t)

	m1 := NewMap()
	m2 := m1
	m3 := NewMap()

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

	m1 = NewMap(String("foo"), Number(0.0), String("bar"), NewList())
	m2 = m2.Edit().Set(String("foo"), Number(0.0)).Set(String("bar"), NewList()).Map(nil)
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

	b1 := NewBlob(bytes.NewBufferString("blob1"))
	b2 := NewBlob(bytes.NewBufferString("blob2"))
	l := []Value{
		Bool(true), String("true"),
		Bool(false), String("false"),
		Number(1), String("Number: 1"),
		Number(0), String("Number: 0"),
		b1, String("blob1"),
		b2, String("blob2"),
		NewList(), String("empty list"),
		NewList(NewList()), String("list of list"),
		NewMap(), String("empty map"),
		NewMap(NewMap(), NewMap()), String("map of map/map"),
		NewSet(), String("empty set"),
		NewSet(NewSet()), String("map of set/set"),
	}
	m1 := NewMap(l...)
	assert.Equal(uint64(12), m1.Len())
	for i := 0; i < len(l); i += 2 {
		assert.True(m1.Get(l[i]).Equals(l[i+1]))
	}
	assert.Nil(m1.Get(Number(42)))
}

func testMapOrder(assert *assert.Assertions, keyType, valueType *Type, tuples []Value, expectOrdering []Value) {
	m := NewMap(tuples...)
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

	testMapOrder(assert,
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

	testMapOrder(assert,
		NumberType, StringType,
		[]Value{
			Number(0), String("unused"),
			Number(1000), String("unused"),
			Number(1), String("unused"),
			Number(100), String("unused"),
			Number(2), String("unused"),
			Number(10), String("unused"),
		},
		[]Value{
			Number(0),
			Number(1),
			Number(2),
			Number(10),
			Number(100),
			Number(1000),
		},
	)

	testMapOrder(assert,
		NumberType, StringType,
		[]Value{
			Number(0), String("unused"),
			Number(-30), String("unused"),
			Number(25), String("unused"),
			Number(1002), String("unused"),
			Number(-5050), String("unused"),
			Number(23), String("unused"),
		},
		[]Value{
			Number(-5050),
			Number(-30),
			Number(0),
			Number(23),
			Number(25),
			Number(1002),
		},
	)

	testMapOrder(assert,
		NumberType, StringType,
		[]Value{
			Number(0.0001), String("unused"),
			Number(0.000001), String("unused"),
			Number(1), String("unused"),
			Number(25.01e3), String("unused"),
			Number(-32.231123e5), String("unused"),
			Number(23), String("unused"),
		},
		[]Value{
			Number(-32.231123e5),
			Number(0.000001),
			Number(0.0001),
			Number(1),
			Number(23),
			Number(25.01e3),
		},
	)

	testMapOrder(assert,
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

	testMapOrder(assert,
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

	me := NewMap().Edit()
	empty := func() bool {
		return me.Map(nil).Empty()
	}

	assert.True(empty())
	me.Set(Bool(false), String("hi"))
	assert.False(empty())
	me.Set(NewList(), NewMap())
	assert.False(empty())
}

func TestMapType(t *testing.T) {
	assert := assert.New(t)

	emptyMapType := MakeMapType(MakeUnionType(), MakeUnionType())
	m := NewMap()
	assert.True(TypeOf(m).Equals(emptyMapType))

	m2 := m.Edit().Remove(String("B")).Map(nil)
	assert.True(emptyMapType.Equals(TypeOf(m2)))

	tr := MakeMapType(StringType, NumberType)
	m2 = m.Edit().Set(String("A"), Number(1)).Map(nil)
	assert.True(tr.Equals(TypeOf(m2)))

	m2 = m.Edit().Set(String("B"), Number(2)).Set(String("C"), Number(2)).Map(nil)
	assert.True(tr.Equals(TypeOf(m2)))

	m3 := m2.Edit().Set(String("A"), Bool(true)).Map(nil)
	assert.True(MakeMapType(StringType, MakeUnionType(BoolType, NumberType)).Equals(TypeOf(m3)), TypeOf(m3).Describe())
	m4 := m3.Edit().Set(Bool(true), Number(1)).Map(nil)
	assert.True(MakeMapType(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, NumberType)).Equals(TypeOf(m4)))
}

func TestMapChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewMap(Number(0), Number(1))
	c1 := getChunks(l1)
	assert.Len(c1, 0)

	l2 := NewMap(NewRef(Number(0)), Number(1))
	c2 := getChunks(l2)
	assert.Len(c2, 1)

	l3 := NewMap(Number(0), NewRef(Number(1)))
	c3 := getChunks(l3)
	assert.Len(c3, 1)
}

func TestMapFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		kvs = append(kvs, Number(i), Number(i+1))
	}

	m := NewMap(kvs...)
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
		k := vs.WriteValue(NewStruct("num", StructData{"n": Number(i)}))
		v := vs.WriteValue(NewStruct("num", StructData{"n": Number(i + 1)}))
		assert.NotNil(k)
		assert.NotNil(v)
		kvs = append(kvs, k, v)
	}

	m := NewMap(kvs...)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(deriveCollectionHeight(m)+1, getRefHeightOfCollection(m))
}

func TestMapModifyAfterRead(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	m := getTestNativeOrderMap(2).toMap()
	// Drop chunk values.
	m = vs.ReadValue(vs.WriteValue(m).TargetHash()).(Map)
	// Modify/query. Once upon a time this would crash.
	fst, fstval := m.First()
	m = m.Edit().Remove(fst).Map(nil)
	assert.False(m.Has(fst))

	fst2, _ := m.First()
	assert.True(m.Has(fst2))

	m = m.Edit().Set(fst, fstval).Map(nil)
	assert.True(m.Has(fst))
}

func TestMapTypeAfterMutations(t *testing.T) {
	assert := assert.New(t)

	test := func(n int, c interface{}) {
		values := make([]Value, 2*n)
		for i := 0; i < n; i++ {
			values[2*i] = Number(i)
			values[2*i+1] = Number(i)
		}

		m := NewMap(values...)
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.sequence())
		assert.True(TypeOf(m).Equals(MakeMapType(NumberType, NumberType)))

		m = m.Edit().Set(String("a"), String("a")).Map(nil)
		assert.Equal(m.Len(), uint64(n+1))
		assert.IsType(c, m.sequence())
		assert.True(TypeOf(m).Equals(MakeMapType(MakeUnionType(NumberType, StringType), MakeUnionType(NumberType, StringType))))

		m = m.Edit().Remove(String("a")).Map(nil)
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.sequence())
		assert.True(TypeOf(m).Equals(MakeMapType(NumberType, NumberType)))
	}

	test(10, mapLeafSequence{})
	test(8000, metaSequence{})
}

func TestCompoundMapWithValuesOfEveryType(t *testing.T) {
	assert := assert.New(t)

	v := Number(42)
	kvs := []Value{
		// Values
		Bool(true), v,
		Number(0), v,
		String("hello"), v,
		NewBlob(bytes.NewBufferString("buf")), v,
		NewSet(Bool(true)), v,
		NewList(Bool(true)), v,
		NewMap(Bool(true), Number(0)), v,
		NewStruct("", StructData{"field": Bool(true)}), v,
		// Refs of values
		NewRef(Bool(true)), v,
		NewRef(Number(0)), v,
		NewRef(String("hello")), v,
		NewRef(NewBlob(bytes.NewBufferString("buf"))), v,
		NewRef(NewSet(Bool(true))), v,
		NewRef(NewList(Bool(true))), v,
		NewRef(NewMap(Bool(true), Number(0))), v,
		NewRef(NewStruct("", StructData{"field": Bool(true)})), v,
	}

	m := NewMap(kvs...)
	for i := 1; m.sequence().isLeaf(); i++ {
		k := Number(i)
		kvs = append(kvs, k, v)
		m = m.Edit().Set(k, v).Map(nil)
	}

	assert.Equal(len(kvs)/2, int(m.Len()))
	fk, fv := m.First()
	assert.True(bool(fk.(Bool)))
	assert.True(v.Equals(fv))

	for i, kOrV := range kvs {
		if i%2 == 0 {
			assert.True(m.Has(kOrV))
			assert.True(v.Equals(m.Get(kOrV)))
		} else {
			assert.True(v.Equals(kOrV))
		}
	}

	for len(kvs) > 0 {
		k := kvs[0]
		kvs = kvs[2:]
		m = m.Edit().Remove(k).Map(nil)
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
		return vs.ReadValue(vs.WriteValue(m).TargetHash()).(Map)
	}

	tm := getTestNativeOrderMap(4)
	nm := tm.toMap()

	for len(tm.entries) > 0 {
		entr := tm.entries
		last := entr[len(entr)-1]
		entr = entr[:len(entr)-1]
		tm.entries = entr
		nm = reload(nm.Edit().Remove(last.key).Map(nil))
		assert.True(tm.toMap().Equals(nm))
	}
}

func TestMapIterFrom(t *testing.T) {
	assert := assert.New(t)

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
	m1 := NewMap(kvs...)
	assert.True(kvs.Equals(test(m1, nil, Number(1000))))
	assert.True(kvs.Equals(test(m1, Number(-1000), Number(1000))))
	assert.True(kvs.Equals(test(m1, Number(-50), Number(1000))))
	assert.True(kvs[2:].Equals(test(m1, Number(-49), Number(1000))))
	assert.True(kvs[2:].Equals(test(m1, Number(-48), Number(1000))))
	assert.True(kvs[4:].Equals(test(m1, Number(-47), Number(1000))))
	assert.True(kvs[98:].Equals(test(m1, Number(48), Number(1000))))
	assert.True(kvs[0:0].Equals(test(m1, Number(100), Number(1000))))
	assert.True(kvs[50:60].Equals(test(m1, Number(0), Number(8))))
}

func TestMapAt(t *testing.T) {
	assert := assert.New(t)

	values := []Value{Bool(false), Number(42), String("a"), String("b"), String("c"), String("d")}
	m := NewMap(values...)

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
	list := NewMap(
		String("one"),
		NewStruct("Foo", StructData{
			"a": Number(1),
		}),
		String("two"),
		NewStruct("Foo", StructData{
			"a": Number(2),
			"b": String("bar"),
		}),
	)
	assert.True(
		MakeMapType(StringType,
			MakeStructType("Foo",
				StructField{"a", NumberType, false},
				StructField{"b", StringType, true},
			),
		).Equals(TypeOf(list)))

	// transpose
	list = NewMap(
		NewStruct("Foo", StructData{
			"a": Number(1),
		}),
		String("one"),
		NewStruct("Foo", StructData{
			"a": Number(2),
			"b": String("bar"),
		}),
		String("two"),
	)
	assert.True(
		MakeMapType(
			MakeStructType("Foo",
				StructField{"a", NumberType, false},
				StructField{"b", StringType, true},
			),
			StringType,
		).Equals(TypeOf(list)))

}

func TestMapWithNil(t *testing.T) {
	assert.Panics(t, func() {
		NewMap(nil, Number(42))
	})
	assert.Panics(t, func() {
		NewSet(Number(42), nil)
	})
	assert.Panics(t, func() {
		NewMap(String("a"), String("b"), nil, Number(42))
	})
	assert.Panics(t, func() {
		NewSet(String("a"), String("b"), Number(42), nil)
	})
}

func TestNestedEditing(t *testing.T) {
	me0 := NewMap().Edit()

	// m.a.a
	me1a := NewMap().Edit()
	me0.Set(String("a"), me1a)
	se2a := NewSet().Edit()
	me1a.Set(String("a"), se2a)
	se2a.Insert(String("a"))

	// m.b.b
	me1b := NewMap().Edit()
	me0.Set(String("b"), me1b)
	se2b := NewSet().Edit()
	me1b.Set(String("b"), se2b)
	se2b.Insert(String("b"))

	mOut := me0.Map(nil)
	assert.True(t, mOut.Equals(NewMap(
		String("a"), NewMap(
			String("a"), NewSet(String("a")),
		),
		String("b"), NewMap(
			String("b"), NewSet(String("b")),
		),
	)))

	se2a.Remove(String("a")).Insert(String("aa"))
	se2b.Remove(String("b")).Insert(String("bb"))

	mOut = me0.Map(nil)
	assert.True(t, mOut.Equals(NewMap(
		String("a"), NewMap(
			String("a"), NewSet(String("aa")),
		),
		String("b"), NewMap(
			String("b"), NewSet(String("bb")),
		),
	)))

	se2a.Remove(String("aa"))
	se2b.Remove(String("bb"))

	mOut = me0.Map(nil)
	assert.True(t, mOut.Equals(NewMap())) // remove empty
}
