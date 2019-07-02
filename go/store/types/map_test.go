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

	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
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
	entries := make([]mapEntry, 0, len(tm.entries.entries))
	entries = append(entries, tm.entries.entries...)
	entries[i].value = v
	return testMap{mapEntrySlice{entries, tm.entries.f}, tm.knownBadKey}
}

func (tm testMap) Remove(from, to int) testMap {
	entries := make([]mapEntry, 0, len(tm.entries.entries)-(to-from))
	entries = append(entries, tm.entries.entries[:from]...)
	entries = append(entries, tm.entries.entries[to:]...)
	return testMap{mapEntrySlice{entries, tm.entries.f}, tm.knownBadKey}
}

func (tm testMap) MaybeGet(key Value) (v Value, ok bool) {
	for _, entry := range tm.entries.entries {
		if entry.key.Equals(Format_7_18, key) {
			return entry.value, true
		}
	}
	return nil, false
}

func (tm testMap) Diff(last testMap) (added []Value, removed []Value, modified []Value) {
	// Note: this could be use tm.toMap/last.toMap and then tmMap.Diff(lastMap) but the
	// purpose of this method is to be redundant.
	if len(tm.entries.entries) == 0 && len(last.entries.entries) == 0 {
		return // nothing changed
	}
	if len(tm.entries.entries) == 0 {
		// everything removed
		for _, entry := range last.entries.entries {
			removed = append(removed, entry.key)
		}
		return
	}
	if len(last.entries.entries) == 0 {
		// everything added
		for _, entry := range tm.entries.entries {
			added = append(added, entry.key)
		}
		return
	}

	for _, entry := range tm.entries.entries {
		otherValue, exists := last.MaybeGet(entry.key)
		if !exists {
			added = append(added, entry.key)
		} else if !entry.value.Equals(Format_7_18, otherValue) {
			modified = append(modified, entry.key)
		}
	}
	for _, entry := range last.entries.entries {
		_, exists := tm.MaybeGet(entry.key)
		if !exists {
			removed = append(removed, entry.key)
		}
	}
	return
}

func (tm testMap) toMap(vrw ValueReadWriter) Map {
	keyvals := []Value{}
	for _, entry := range tm.entries.entries {
		keyvals = append(keyvals, entry.key, entry.value)
	}
	return NewMap(context.Background(), vrw, keyvals...)
}

func toValuable(vs ValueSlice) []Valuable {
	vb := make([]Valuable, len(vs))
	for i, v := range vs {
		vb[i] = v
	}
	return vb
}

func (tm testMap) Flatten(from, to int) []Value {
	flat := make([]Value, 0, len(tm.entries.entries)*2)
	for _, entry := range tm.entries.entries[from:to] {
		flat = append(flat, entry.key)
		flat = append(flat, entry.value)
	}
	return flat
}

func (tm testMap) FlattenAll() []Value {
	return tm.Flatten(0, len(tm.entries.entries))
}

func newSortedTestMap(length int, gen genValueFn) testMap {
	keys := make(ValueSlice, 0, length)
	for i := 0; i < length; i++ {
		keys = append(keys, gen(i))
	}

	sort.Sort(ValueSort{keys, Format_7_18})

	entries := make([]mapEntry, 0, len(keys))
	for i, k := range keys {
		entries = append(entries, mapEntry{k, Float(i * 2)})
	}

	return testMap{mapEntrySlice{entries, Format_7_18}, Float(length + 2)}
}

func newTestMapFromMap(m Map) testMap {
	entries := make([]mapEntry, 0, m.Len())
	m.IterAll(context.Background(), func(key, value Value) {
		entries = append(entries, mapEntry{key, value})
	})
	return testMap{mapEntrySlice{entries, Format_7_18}, Float(-0)}
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

	return testMap{mapEntrySlice{entries, Format_7_18}, gen(mask + 1)}
}

func validateMap(t *testing.T, vrw ValueReadWriter, m Map, entries mapEntrySlice) {
	tm := testMap{entries: entries}
	assert.True(t, m.Equals(Format_7_18, tm.toMap(vrw)))

	out := mapEntrySlice{}
	m.IterAll(context.Background(), func(k Value, v Value) {
		out.entries = append(out.entries, mapEntry{k, v})
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
	tmap := NewMap(context.Background(), vrw, elems.FlattenAll()...)
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
				l2.Iter(context.Background(), func(key, value Value) (stop bool) {
					entry := elems.entries.entries[idx]
					if !key.Equals(Format_7_18, entry.key) {
						// TODO(binformat)
						fmt.Printf("%d: %s (%s)\n!=\n%s (%s)\n", idx, EncodedValue(context.Background(), Format_7_18, key), key.Hash(Format_7_18), EncodedValue(context.Background(), Format_7_18, entry.key), entry.key.Hash(Format_7_18))
						stop = true
					}
					if !value.Equals(Format_7_18, entry.value) {
						// TODO(binformat)
						fmt.Printf("%s (%s) !=\n%s (%s)\n", EncodedValue(context.Background(), Format_7_18, value), value.Hash(Format_7_18), EncodedValue(context.Background(), Format_7_18, entry.value), entry.value.Hash(Format_7_18))
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
				copy(dup[1:], elems.entries.entries)
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(context.Background(), vrw, flat...)
			},
			appendOne: func() Collection {
				dup := make([]mapEntry, length+1)
				copy(dup, elems.entries.entries)
				dup[len(dup)-1] = mapEntry{Float(length*2 + 1), Float((length*2 + 1) * 2)}
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(context.Background(), vrw, flat...)
			},
		},
		elems: elems,
	}
}

func (suite *mapTestSuite) createStreamingMap(vs *ValueStore) Map {
	kvChan := make(chan Value)
	mapChan := NewStreamingMap(context.Background(), vs, kvChan)
	for _, entry := range suite.elems.entries.entries {
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

	entries := mapEntrySlice{make([]mapEntry, len(suite.elems.entries.entries)), Format_7_18}
	copy(entries.entries, suite.elems.entries.entries)
	entries.entries[0], entries.entries[1] = entries.entries[1], entries.entries[0]

	kvChan := make(chan Value, len(entries.entries)*2)
	for _, e := range entries.entries {
		kvChan <- e.key
		kvChan <- e.value
	}
	close(kvChan)

	readInput := func(vrw ValueReadWriter, kvs <-chan Value, outChan chan<- Map) {
		readMapInput(context.Background(), Format_7_18, vrw, kvs, outChan)
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
	return NewStruct(Format_7_18, "", StructData{"n": Float(i)})
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
		return vrw.WriteValue(context.Background(), NewSet(context.Background(), Format_7_18, vrw, Float(i)))
	})
}

func accumulateMapDiffChanges(m1, m2 Map) (added []Value, removed []Value, modified []Value) {
	changes := make(chan ValueChanged)
	go func() {
		m1.Diff(context.Background(), m2, changes, nil)
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
		return temp.NewStruct(Format_7_18, []Value{
			Bool(i%2 == 0),
			Float(i),
			String(fmt.Sprintf("I AM A REALLY REALY REALL SUPER CALIFRAGILISTICLY CRAZY-ASSED LONGTASTIC String %d", i)),
			String(fmt.Sprintf("I am a bit shorter and also more chill: %d", i)),
		})
	}

	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := newValueStoreWithCacheAndPending(cs, 0, 0)

	numEdits := 10000
	vals := make([]Value, 0, numEdits)
	me := NewMap(context.Background(), vs).Edit()
	for i := 0; i < 10000; i++ {
		s := newLargeStruct(i)
		vals = append(vals, s)
		me.Set(Float(i), s)
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
			s := vals[i].(Struct)
			s = s.Set("Number", Float(float64(s.Get("Number").(Float))+1))
			me.Set(k, s)
		}
		i++
	}

	cs.Writes = 0
	cs.Reads = 0

	m = me.Map(context.Background())

	vs.Commit(context.Background(), vs.Root(context.Background()), vs.Root(context.Background()))

	assert.Equal(t, uint64(3), NewRef(m, Format_7_18).Height())
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

	me := NewMap(context.Background(), vrw).Edit()

	for i := 0; i < 10000; i++ {
		me.Set(String(prefix+fmt.Sprintf("%d", i)), Float(i))
	}

	me.Map(context.Background())
}

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m := NewMap(context.Background(), vrw)
	assert.Equal(uint64(0), m.Len())
	m = NewMap(context.Background(), vrw, String("foo1"), String("bar1"), String("foo2"), String("bar2"))
	assert.Equal(uint64(2), m.Len())
	assert.True(String("bar1").Equals(Format_7_18, m.Get(context.Background(), String("foo1"))))
	assert.True(String("bar2").Equals(Format_7_18, m.Get(context.Background(), String("foo2"))))
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
	m := NewMap(context.Background(), vrw, l...)
	assert.Equal(uint64(3), m.Len())
	assert.True(String("foo").Equals(Format_7_18, m.Get(context.Background(), String("hello"))))
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
	m := NewMap(context.Background(), vrw, l...)
	assert.Equal(uint64(4), m.Len())
	assert.True(Float(5).Equals(Format_7_18, m.Get(context.Background(), Float(1))))
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
		for _, entry := range tm.entries.entries {
			k, v := entry.key, entry.value
			assert.True(m.Has(context.Background(), k))
			assert.True(m.Get(context.Background(), k).Equals(Format_7_18, v))
			assert.True(m2.Has(context.Background(), k))
			assert.True(m2.Get(context.Background(), k).Equals(Format_7_18, v))
		}
		diffMapTest(assert, m, m2, 0, 0, 0)
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func hasAll(m Map, keys ...string) bool {
	ctx := context.Background()
	for _, k := range keys {
		if !m.Has(ctx, String(k)) {
			return false
		}
	}

	return true
}

func hasNone(m Map, keys ...string) bool {
	ctx := context.Background()
	for _, k := range keys {
		if m.Has(ctx, String(k)) {
			return false
		}
	}

	return true
}

func TestMapHasRemove(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	me := NewMap(context.Background(), vrw).Edit()

	initial := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"}
	initialUnexpected := []string{"n", "o", "p", "q", "r", "s"}
	for _, k := range initial {
		me.Set(String(k), Int(0))
	}

	m := me.Map(context.Background())
	assert.True(m.Len() == uint64(len(initial)))
	assert.True(hasAll(m, initial...))
	assert.True(hasNone(m, initialUnexpected...))

	me = m.Edit()
	// add new
	me.Set(String("n"), Int(1))

	// remove
	me.Remove(String("b"))

	// remove and re-add
	me.Remove(String("c"))
	me.Set(String("c"), Int(1))

	// set then remove
	me.Set(String("d"), Int(2))
	me.Remove(String("d"))

	// add then remove
	me.Set(String("o"), Int(1))
	me.Remove(String("o"))

	// In-order insertions
	me.Set(String("p"), Int(1))
	me.Set(String("q"), Int(1))

	// Out-of-order insertions
	me.Set(String("s"), Int(1))
	me.Set(String("r"), Int(1))

	m = me.Map(context.Background())
	expected := []string{"a", "c", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "p", "q", "r", "s"}
	unexpected := []string{"b", "d", "o"}
	assert.True(hasAll(m, expected...))
	assert.True(hasNone(m, unexpected...))

	assert.True(m.Len() == uint64(len(expected)))
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
			actual := whole.Edit().Remove(tm.entries.entries[i].key).Map(context.Background())
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(Format_7_18, actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries.entries); i += incr {
			run(i)
		}
		run(len(tm.entries.entries) - 1)
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
	assert.True(original.Equals(Format_7_18, actual))
}

func TestMapFirst(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	m1 := NewMap(context.Background(), vrw)
	k, v := m1.First(context.Background())
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(context.Background())
	ak, av := m1.First(context.Background())
	var ek, ev Value

	m1.Iter(context.Background(), func(k, v Value) (stop bool) {
		ek, ev = k, v
		return true
	})

	assert.True(ek.Equals(Format_7_18, ak))
	assert.True(ev.Equals(Format_7_18, av))
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
		actualKey, actualValue := m.First(context.Background())
		assert.True(tm.entries.entries[0].key.Equals(Format_7_18, actualKey))
		assert.True(tm.entries.entries[0].value.Equals(Format_7_18, actualValue))
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

	m1 := NewMap(context.Background(), vrw)
	k, v := m1.First(context.Background())
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(context.Background())
	ak, av := m1.Last(context.Background())
	var ek, ev Value

	m1.Iter(context.Background(), func(k, v Value) (stop bool) {
		ek, ev = k, v
		return false
	})

	assert.True(ek.Equals(Format_7_18, ak))
	assert.True(ev.Equals(Format_7_18, av))
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
		actualKey, actualValue := m.Last(context.Background())
		assert.True(tm.entries.entries[len(tm.entries.entries)-1].key.Equals(Format_7_18, actualKey))
		assert.True(tm.entries.entries[len(tm.entries.entries)-1].value.Equals(Format_7_18, actualValue))
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
	ctx := context.Background()

	assertMapVal := func(me *MapEditor, k, expectedVal Value) *MapEditor {
		m := me.Map(ctx)
		mV := m.Get(ctx, k)
		assert.True((expectedVal == nil && mV == nil) || expectedVal.Equals(Format_7_18, mV))
		return m.Edit()
	}

	me := NewMap(ctx, vrw).Edit()
	me = assertMapVal(me, String("a"), nil)

	me.Set(String("a"), Float(42))
	me = assertMapVal(me, String("a"), Float(42))

	me.Set(String("a"), Float(43))
	me = assertMapVal(me, String("a"), Float(43))

	me.Remove(String("a"))
	me = assertMapVal(me, String("a"), nil)

	// in-order insertions
	me.Set(String("b"), Float(43))
	me.Set(String("c"), Float(44))

	me = assertMapVal(me, String("b"), Float(43))
	me = assertMapVal(me, String("c"), Float(44))

	// out-of-order insertions
	me.Set(String("z"), Float(0))
	me.Set(String("y"), Float(1))

	me = assertMapVal(me, String("z"), Float(0))
	me = assertMapVal(me, String("y"), Float(1))

	// removals
	me.Remove(String("z"))
	me.Remove(String("a"))
	me.Remove(String("y"))
	me.Remove(String("b"))
	me.Remove(String("c"))

	me = assertMapVal(me, String("a"), nil)
	me = assertMapVal(me, String("b"), nil)
	me = assertMapVal(me, String("c"), nil)
	me = assertMapVal(me, String("y"), nil)
	me = assertMapVal(me, String("z"), nil)
	me = assertMapVal(me, String("never-inserted"), nil)

	m := me.Map(context.Background())
	assert.True(m.Len() == 0)
}

func validateMapInsertion(t *testing.T, tm testMap) {
	vrw := newTestValueStore()
	ctx := context.Background()

	allMe := NewMap(context.Background(), vrw).Edit()
	incrMe := NewMap(context.Background(), vrw).Edit()

	for _, entry := range tm.entries.entries {
		allMe.Set(entry.key, entry.value)
		incrMe.Set(entry.key, entry.value)

		incrMe = incrMe.Map(ctx).Edit()
	}

	m1 := allMe.Map(ctx)
	m2 := incrMe.Map(ctx)

	validateMap(t, vrw, m1, tm.entries)
	validateMap(t, vrw, m2, tm.entries)
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
			assert.True(expected.Equals(Format_7_18, actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries.entries)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(tm.entries.entries)-offset, len(tm.entries.entries))
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

	m1 := NewMap(context.Background(), vrw)
	m2 := m1.Edit().SetM().Map(context.Background())
	assert.True(m1.Equals(Format_7_18, m2))
	m3 := m2.Edit().SetM(String("foo"), String("bar"), String("hot"), String("dog")).Map(context.Background())
	assert.Equal(uint64(2), m3.Len())
	assert.True(String("bar").Equals(Format_7_18, m3.Get(context.Background(), String("foo"))))
	assert.True(String("dog").Equals(Format_7_18, m3.Get(context.Background(), String("hot"))))
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
	for i, entry := range tm.entries.entries {
		newValue := Float(int64(entry.value.(Float)) + 1)
		expectedWorking = expectedWorking.SetValue(i, newValue)
		actual = actual.Edit().Set(entry.key, newValue).Map(context.Background())
	}

	expected := expectedWorking.toMap(vrw)
	assert.Equal(expected.Len(), actual.Len())
	assert.True(expected.Equals(Format_7_18, actual))
	assert.False(original.Equals(Format_7_18, actual))
	diffMapTest(assert, expected, actual, 0, 0, 0)
}

// BUG 98
func TestMapDuplicateSet(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m1 := NewMap(context.Background(), vrw, Bool(true), Bool(true), Float(42), Float(42), Float(42), Float(42))
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
		for _, entry := range tm.entries.entries {
			v, ok := m.MaybeGet(context.Background(), entry.key)
			if assert.True(ok, "%v should have been in the map!", entry.key) {
				assert.True(v.Equals(Format_7_18, entry.value), "%v != %v", v, entry.value)
			}
		}
		_, ok := m.MaybeGet(context.Background(), tm.knownBadKey)
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

	m := NewMap(context.Background(), vrw)

	type entry struct {
		key   Value
		value Value
	}

	type resultList []entry
	results := resultList{}
	got := func(key, val Value) bool {
		for _, r := range results {
			if key.Equals(Format_7_18, r.key) && val.Equals(Format_7_18, r.value) {
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

	m.Iter(context.Background(), cb)
	assert.Equal(0, len(results))

	m = m.Edit().Set(String("a"), Float(0)).Set(String("b"), Float(1)).Map(context.Background())
	m.Iter(context.Background(), cb)
	assert.Equal(2, len(results))
	assert.True(got(String("a"), Float(0)))
	assert.True(got(String("b"), Float(1)))

	results = resultList{}
	stop = true
	m.Iter(context.Background(), cb)
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

		m.Iter(context.Background(), func(k, v Value) (done bool) {
			assert.True(tm.entries.entries[idx].key.Equals(Format_7_18, k))
			assert.True(tm.entries.entries[idx].value.Equals(Format_7_18, v))
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
		return k.Equals(Format_7_18, String("foo")) && v.Equals(Format_7_18, String("bar"))
	}

	assert.False(NewMap(context.Background(), vrw).Any(context.Background(), p))
	assert.False(NewMap(context.Background(), vrw, String("foo"), String("baz")).Any(context.Background(), p))
	assert.True(NewMap(context.Background(), vrw, String("foo"), String("bar")).Any(context.Background(), p))
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

		m.IterAll(context.Background(), func(k, v Value) {
			assert.True(tm.entries.entries[idx].key.Equals(Format_7_18, k))
			assert.True(tm.entries.entries[idx].value.Equals(Format_7_18, v))
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

	m1 := NewMap(context.Background(), vrw)
	m2 := m1
	m3 := NewMap(context.Background(), vrw)

	assert.True(m1.Equals(Format_7_18, m2))
	assert.True(m2.Equals(Format_7_18, m1))
	assert.True(m3.Equals(Format_7_18, m2))
	assert.True(m2.Equals(Format_7_18, m3))
	diffMapTest(assert, m1, m2, 0, 0, 0)
	diffMapTest(assert, m1, m3, 0, 0, 0)
	diffMapTest(assert, m2, m1, 0, 0, 0)
	diffMapTest(assert, m2, m3, 0, 0, 0)
	diffMapTest(assert, m3, m1, 0, 0, 0)
	diffMapTest(assert, m3, m2, 0, 0, 0)

	// TODO(binformat)
	m1 = NewMap(context.Background(), vrw, String("foo"), Float(0.0), String("bar"), NewList(context.Background(), vrw))
	m2 = m2.Edit().Set(String("foo"), Float(0.0)).Set(String("bar"), NewList(context.Background(), vrw)).Map(context.Background())
	assert.True(m1.Equals(Format_7_18, m2))
	assert.True(m2.Equals(Format_7_18, m1))
	assert.False(m2.Equals(Format_7_18, m3))
	assert.False(m3.Equals(Format_7_18, m2))
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

	// TODO(binformat)
	b1 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("blob1"))
	b2 := NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("blob2"))
	l := []Value{
		Bool(true), String("true"),
		Bool(false), String("false"),
		Float(1), String("Float: 1"),
		Float(0), String("Float: 0"),
		b1, String("blob1"),
		b2, String("blob2"),
		NewList(context.Background(), vrw), String("empty list"),
		NewList(context.Background(), vrw, NewList(context.Background(), vrw)), String("list of list"),
		NewMap(context.Background(), vrw), String("empty map"),
		NewMap(context.Background(), vrw, NewMap(context.Background(), vrw), NewMap(context.Background(), vrw)), String("map of map/map"),
		NewSet(context.Background(), Format_7_18, vrw), String("empty set"),
		NewSet(context.Background(), Format_7_18, vrw, NewSet(context.Background(), Format_7_18, vrw)), String("map of set/set"),
	}
	m1 := NewMap(context.Background(), vrw, l...)
	assert.Equal(uint64(12), m1.Len())
	for i := 0; i < len(l); i += 2 {
		assert.True(m1.Get(context.Background(), l[i]).Equals(Format_7_18, l[i+1]))
	}
	assert.Nil(m1.Get(context.Background(), Float(42)))
}

func testMapOrder(assert *assert.Assertions, vrw ValueReadWriter, keyType, valueType *Type, tuples []Value, expectOrdering []Value) {
	m := NewMap(context.Background(), vrw, tuples...)
	i := 0
	m.IterAll(context.Background(), func(key, value Value) {
		// TODO(binformat)
		assert.Equal(expectOrdering[i].Hash(Format_7_18).String(), key.Hash(Format_7_18).String())
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
	ctx := context.Background()

	me := NewMap(ctx, vrw).Edit()

	m := me.Map(ctx)
	me = m.Edit()
	assert.True(m.Empty())

	me.Set(Bool(false), String("hi"))
	m = me.Map(ctx)
	me = m.Edit()
	assert.False(m.Empty())

	// TODO(binformat)
	me.Set(NewList(ctx, vrw), NewMap(ctx, vrw))
	m = me.Map(ctx)
	assert.False(m.Empty())
}

func TestMapType(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	emptyMapType := MakeMapType(MakeUnionType(), MakeUnionType())
	m := NewMap(context.Background(), vrw)
	assert.True(TypeOf(m).Equals(Format_7_18, emptyMapType))

	m2 := m.Edit().Remove(String("B")).Map(context.Background())
	assert.True(emptyMapType.Equals(Format_7_18, TypeOf(m2)))

	tr := MakeMapType(StringType, FloaTType)
	m2 = m.Edit().Set(String("A"), Float(1)).Map(context.Background())
	assert.True(tr.Equals(Format_7_18, TypeOf(m2)))

	m2 = m.Edit().Set(String("B"), Float(2)).Set(String("C"), Float(2)).Map(context.Background())
	assert.True(tr.Equals(Format_7_18, TypeOf(m2)))

	m3 := m2.Edit().Set(String("A"), Bool(true)).Map(context.Background())
	assert.True(MakeMapType(StringType, MakeUnionType(BoolType, FloaTType)).Equals(Format_7_18, TypeOf(m3)), TypeOf(m3).Describe(context.Background(), Format_7_18))
	m4 := m3.Edit().Set(Bool(true), Float(1)).Map(context.Background())
	assert.True(MakeMapType(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, FloaTType)).Equals(Format_7_18, TypeOf(m4)))
}

func TestMapChunks(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	l1 := NewMap(context.Background(), vrw, Float(0), Float(1))
	c1 := getChunks(l1)
	assert.Len(c1, 0)

	l2 := NewMap(context.Background(), vrw, NewRef(Float(0), Format_7_18), Float(1))
	c2 := getChunks(l2)
	assert.Len(c2, 1)

	l3 := NewMap(context.Background(), vrw, Float(0), NewRef(Float(1), Format_7_18))
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

	m := NewMap(context.Background(), vrw, kvs...)
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
		k := vs.WriteValue(context.Background(), NewStruct(Format_7_18, "num", StructData{"n": Float(i)}))
		v := vs.WriteValue(context.Background(), NewStruct(Format_7_18, "num", StructData{"n": Float(i + 1)}))
		assert.NotNil(k)
		assert.NotNil(v)
		kvs = append(kvs, k, v)
	}

	m := NewMap(context.Background(), vs, kvs...)
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
	fst, fstval := m.First(context.Background())
	m = m.Edit().Remove(fst).Map(context.Background())
	assert.False(m.Has(context.Background(), fst))

	fst2, _ := m.First(context.Background())
	assert.True(m.Has(context.Background(), fst2))

	m = m.Edit().Set(fst, fstval).Map(context.Background())
	assert.True(m.Has(context.Background(), fst))
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

		m := NewMap(context.Background(), vrw, values...)
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.asSequence())
		assert.True(TypeOf(m).Equals(Format_7_18, MakeMapType(FloaTType, FloaTType)))

		m = m.Edit().Set(String("a"), String("a")).Map(context.Background())
		assert.Equal(m.Len(), uint64(n+1))
		assert.IsType(c, m.asSequence())
		assert.True(TypeOf(m).Equals(Format_7_18, MakeMapType(MakeUnionType(FloaTType, StringType), MakeUnionType(FloaTType, StringType))))

		m = m.Edit().Remove(String("a")).Map(context.Background())
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.asSequence())
		assert.True(TypeOf(m).Equals(Format_7_18, MakeMapType(FloaTType, FloaTType)))
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
		// TODO(binformat)
		NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("buf")), v,
		NewSet(context.Background(), Format_7_18, vrw, Bool(true)), v,
		NewList(context.Background(), vrw, Bool(true)), v,
		NewMap(context.Background(), vrw, Bool(true), Float(0)), v,
		NewStruct(Format_7_18, "", StructData{"field": Bool(true)}), v,
		// Refs of values
		NewRef(Bool(true), Format_7_18), v,
		NewRef(Float(0), Format_7_18), v,
		NewRef(String("hello"), Format_7_18), v,
		// TODO(binformat)
		NewRef(NewBlob(context.Background(), Format_7_18, vrw, bytes.NewBufferString("buf")), Format_7_18), v,
		NewRef(NewSet(context.Background(), Format_7_18, vrw, Bool(true)), Format_7_18), v,
		NewRef(NewList(context.Background(), vrw, Bool(true)), Format_7_18), v,
		NewRef(NewMap(context.Background(), vrw, Bool(true), Float(0)), Format_7_18), v,
		NewRef(NewStruct(Format_7_18, "", StructData{"field": Bool(true)}), Format_7_18), v,
	}

	m := NewMap(context.Background(), vrw, kvs...)
	for i := 1; m.asSequence().isLeaf(); i++ {
		k := Float(i)
		kvs = append(kvs, k, v)
		m = m.Edit().Set(k, v).Map(context.Background())
	}

	assert.Equal(len(kvs)/2, int(m.Len()))
	fk, fv := m.First(context.Background())
	assert.True(bool(fk.(Bool)))
	assert.True(v.Equals(Format_7_18, fv))

	for i, keyOrValue := range kvs {
		if i%2 == 0 {
			assert.True(m.Has(context.Background(), keyOrValue))
			assert.True(v.Equals(Format_7_18, m.Get(context.Background(), keyOrValue)))
		} else {
			assert.True(v.Equals(Format_7_18, keyOrValue))
		}
	}

	for len(kvs) > 0 {
		k := kvs[0]
		kvs = kvs[2:]
		m = m.Edit().Remove(k).Map(context.Background())
		assert.False(m.Has(context.Background(), k))
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

	for len(tm.entries.entries) > 0 {
		entr := tm.entries.entries
		last := entr[len(entr)-1]
		entr = entr[:len(entr)-1]
		tm.entries.entries = entr
		nm = reload(nm.Edit().Remove(last.key).Map(context.Background()))
		assert.True(tm.toMap(vs).Equals(Format_7_18, nm))
	}
}

func TestMapIterFrom(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	test := func(m Map, start, end Value) ValueSlice {
		res := ValueSlice{}
		m.IterFrom(context.Background(), start, func(k, v Value) bool {
			// TODO(binformat)
			if end.Less(Format_7_18, k) {
				return true
			}
			res = append(res, k, v)
			return false
		})
		return res
	}

	kvs := generateNumbersAsValuesFromToBy(-50, 50, 1)
	m1 := NewMap(context.Background(), vrw, kvs...)
	assert.True(kvs.Equals(Format_7_18, test(m1, nil, Float(1000))))
	assert.True(kvs.Equals(Format_7_18, test(m1, Float(-1000), Float(1000))))
	assert.True(kvs.Equals(Format_7_18, test(m1, Float(-50), Float(1000))))
	assert.True(kvs[2:].Equals(Format_7_18, test(m1, Float(-49), Float(1000))))
	assert.True(kvs[2:].Equals(Format_7_18, test(m1, Float(-48), Float(1000))))
	assert.True(kvs[4:].Equals(Format_7_18, test(m1, Float(-47), Float(1000))))
	assert.True(kvs[98:].Equals(Format_7_18, test(m1, Float(48), Float(1000))))
	assert.True(kvs[0:0].Equals(Format_7_18, test(m1, Float(100), Float(1000))))
	assert.True(kvs[50:60].Equals(Format_7_18, test(m1, Float(0), Float(8))))
}

func TestMapAt(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	values := []Value{Bool(false), Float(42), String("a"), String("b"), String("c"), String("d")}
	m := NewMap(context.Background(), vrw, values...)

	for i := 0; i < len(values); i += 2 {
		k, v := m.At(context.Background(), uint64(i/2))
		assert.Equal(values[i], k)
		assert.Equal(values[i+1], v)
	}

	assert.Panics(func() {
		m.At(context.Background(), 42)
	})
}

func TestMapWithStructShouldHaveOptionalFields(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	list := NewMap(context.Background(), vrw,
		String("one"),
		NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(1),
		}),
		String("two"),
		NewStruct(Format_7_18, "Foo", StructData{
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
		).Equals(Format_7_18, TypeOf(list)))

	// transpose
	list = NewMap(context.Background(), vrw,
		NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(1),
		}),
		String("one"),
		NewStruct(Format_7_18, "Foo", StructData{
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
		).Equals(Format_7_18, TypeOf(list)))

}

func TestMapWithNil(t *testing.T) {
	vrw := newTestValueStore()

	assert.Panics(t, func() {
		NewMap(context.Background(), nil, Float(42))
	})
	assert.Panics(t, func() {
		NewSet(context.Background(), Format_7_18, vrw, Float(42), nil)
	})
	assert.Panics(t, func() {
		NewMap(context.Background(), vrw, String("a"), String("b"), nil, Float(42))
	})
	assert.Panics(t, func() {
		NewSet(context.Background(), Format_7_18, vrw, String("a"), String("b"), Float(42), nil)
	})
}
