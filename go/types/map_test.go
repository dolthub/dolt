// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

const testMapSize = 1000

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

func newMapTestSuite(size uint, expectRefStr string, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int, gen genValueFn) *mapTestSuite {
	length := 1 << size
	keyType := gen(0).Type()
	elems := newSortedTestMap(length, gen)
	tr := MakeMapType(keyType, NumberType)
	tmap := NewMap(elems.FlattenAll()...)
	return &mapTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    tmap,
			expectType:             tr,
			expectLen:              uint64(length),
			expectRef:              expectRefStr,
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				l2 := v2.(Map)
				out := ValueSlice{}
				l2.IterAll(func(key, value Value) {
					out = append(out, key, value)
				})
				return ValueSlice(elems.FlattenAll()).Equals(out)
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

func (suite *mapTestSuite) createStreamingMap(vs *ValueStore) {
	randomized := make(mapEntrySlice, len(suite.elems.entries))
	for i, j := range rand.Perm(len(randomized)) {
		randomized[j] = suite.elems.entries[i]
	}

	kvChan := make(chan Value)
	mapChan := NewStreamingMap(vs, kvChan)
	for _, entry := range randomized {
		kvChan <- entry.key
		kvChan <- entry.value
	}
	close(kvChan)
	suite.True(suite.validate(<-mapChan), "map not valid")
}

func (suite *mapTestSuite) TestStreamingMap() {
	vs := NewTestValueStore()
	defer vs.Close()
	suite.createStreamingMap(vs)
}

func (suite *mapTestSuite) TestStreamingMap2() {
	wg := sync.WaitGroup{}
	vs := NewTestValueStore()
	defer vs.Close()

	wg.Add(2)
	go func() {
		suite.createStreamingMap(vs)
		wg.Done()
	}()
	go func() {
		suite.createStreamingMap(vs)
		wg.Done()
	}()
	wg.Wait()
}

func TestMapSuite1K(t *testing.T) {
	suite.Run(t, newMapTestSuite(10, "chqe8pkmi2lhn2buvqai357pgp3sg3t6", 3, 2, 2, newNumber))
}

func TestMapSuite4K(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, "v6qlscpd5j6ba89v5ebkijgci2djcpls", 7, 2, 2, newNumber))
}

func TestMapSuite1KStructs(t *testing.T) {
	suite.Run(t, newMapTestSuite(10, "20b1927mnjqa0aqsn4lf5rv80rdnebru", 3, 2, 2, newNumberStruct))
}

func TestMapSuite4KStructs(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, "q2kgo4jonhgfeoblvlovh2eo7kqjel54", 7, 2, 2, newNumberStruct))
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
			added = append(added, change.V)
		} else if change.ChangeType == DiffChangeRemoved {
			removed = append(removed, change.V)
		} else {
			modified = append(modified, change.V)
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

	vs := NewTestValueStore()
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
	m1 := NewMap()
	assert.False(m1.Has(String("foo")))
	m2 := m1.Set(String("foo"), String("foo"))
	assert.False(m1.Has(String("foo")))
	assert.True(m2.Has(String("foo")))
	m3 := m1.Remove(String("foo"))
	assert.False(m1.Has(String("foo")))
	assert.True(m2.Has(String("foo")))
	assert.False(m3.Has(String("foo")))
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
			actual := whole.Remove(tm.entries[i].key)
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
	doTest(64, getTestRefToNativeOrderMap(4, NewTestValueStore()))
	doTest(64, getTestRefToValueOrderMap(4, NewTestValueStore()))
}

func TestMapRemoveNonexistentKey(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	tm := getTestNativeOrderMap(2)
	original := tm.toMap()
	actual := original.Remove(Number(-1)) // rand.Int63 returns non-negative numbers.

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

	m1 = m1.Set(String("foo"), String("bar"))
	m1 = m1.Set(String("hot"), String("dog"))
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
	doTest(getTestRefToNativeOrderMap(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, NewTestValueStore()))
}

func TestMapLast(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	m1 := NewMap()
	k, v := m1.First()
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Set(String("foo"), String("bar"))
	m1 = m1.Set(String("hot"), String("dog"))
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
	doTest(getTestRefToNativeOrderMap(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, NewTestValueStore()))
}

func TestMapSetGet(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	m1 := NewMap()
	assert.Nil(m1.Get(String("foo")))
	m2 := m1.Set(String("foo"), Number(42))
	assert.Nil(m1.Get(String("foo")))
	assert.True(Number(42).Equals(m2.Get(String("foo"))))
	m3 := m2.Set(String("foo"), Number(43))
	assert.Nil(m1.Get(String("foo")))
	assert.True(Number(42).Equals(m2.Get(String("foo"))))
	assert.True(Number(43).Equals(m3.Get(String("foo"))))
	m4 := m3.Remove(String("foo"))
	assert.Nil(m1.Get(String("foo")))
	assert.True(Number(42).Equals(m2.Get(String("foo"))))
	assert.True(Number(43).Equals(m3.Get(String("foo"))))
	assert.Nil(m4.Get(String("foo")))
}

func validateMapInsertion(t *testing.T, tm testMap) {
	m := NewMap()
	for i, entry := range tm.entries {
		m = m.Set(entry.key, entry.value)
		validateMap(t, m, tm.entries[0:i+1])
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
			actual := tm.Remove(from, to).toMap().SetM(tm.Flatten(from, to)...)
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
	doTest(64, 1, getTestRefToNativeOrderMap(4, NewTestValueStore()))
	doTest(64, 1, getTestRefToValueOrderMap(4, NewTestValueStore()))
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
		actual = actual.Set(entry.key, newValue)
	}

	expected := expectedWorking.toMap()
	assert.Equal(expected.Len(), actual.Len())
	assert.True(expected.Equals(actual))
	assert.False(original.Equals(actual))
	diffMapTest(assert, expected, actual, 0, 0, 0)
}

func TestMapSetM(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	m2 := m1.SetM()
	assert.True(m1.Equals(m2))
	m3 := m2.SetM(String("foo"), String("bar"), String("hot"), String("dog"))
	assert.Equal(uint64(2), m3.Len())
	assert.True(String("bar").Equals(m3.Get(String("foo"))))
	assert.True(String("dog").Equals(m3.Get(String("hot"))))
	m4 := m3.SetM(String("mon"), String("key"))
	assert.Equal(uint64(2), m3.Len())
	assert.Equal(uint64(3), m4.Len())
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
	doTest(getTestRefToNativeOrderMap(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, NewTestValueStore()))
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

	m = m.SetM(String("a"), Number(0), String("b"), Number(1))
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
	doTest(getTestRefToNativeOrderMap(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, NewTestValueStore()))
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
	doTest(getTestRefToNativeOrderMap(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, NewTestValueStore()))
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
	m2 = m2.SetM(String("foo"), Number(0.0), String("bar"), NewList())
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

	m := NewMap()
	assert.True(m.Empty())
	m = m.Set(Bool(false), String("hi"))
	assert.False(m.Empty())
	m = m.Set(NewList(), NewMap())
	assert.False(m.Empty())
}

func TestMapType(t *testing.T) {
	assert := assert.New(t)

	emptyMapType := MakeMapType(MakeUnionType(), MakeUnionType())
	m := NewMap()
	assert.True(m.Type().Equals(emptyMapType))

	m2 := m.Remove(String("B"))
	assert.True(emptyMapType.Equals(m2.Type()))

	tr := MakeMapType(StringType, NumberType)
	m2 = m.Set(String("A"), Number(1))
	assert.True(tr.Equals(m2.Type()))

	m2 = m.SetM(String("B"), Number(2), String("C"), Number(2))
	assert.True(tr.Equals(m2.Type()))

	m3 := m2.Set(String("A"), Bool(true))
	assert.True(MakeMapType(StringType, MakeUnionType(BoolType, NumberType)).Equals(m3.Type()), m3.Type().Describe())
	m4 := m3.Set(Bool(true), Number(1))
	assert.True(MakeMapType(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, NumberType)).Equals(m4.Type()))
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
	assert.Equal("jmtmv5mjipjrt5s6s6d80louisqhnj62", m.Hash().String())
	assert.Equal(deriveCollectionHeight(m), getRefHeightOfCollection(m))
}

func TestMapRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)
	vs := NewTestValueStore()

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		k := vs.WriteValue(NewStruct("num", StructData{"n": Number(i)}))
		v := vs.WriteValue(NewStruct("num", StructData{"n": Number(i + 1)}))
		assert.NotNil(k)
		assert.NotNil(v)
		kvs = append(kvs, k, v)
	}

	m := NewMap(kvs...)
	assert.Equal("g49bom2pq40n2v927846vpmc3injuf5a", m.Hash().String())
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(deriveCollectionHeight(m)+1, getRefHeightOfCollection(m))
}

func TestMapModifyAfterRead(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := NewTestValueStore()
	m := getTestNativeOrderMap(2).toMap()
	// Drop chunk values.
	m = vs.ReadValue(vs.WriteValue(m).TargetHash()).(Map)
	// Modify/query. Once upon a time this would crash.
	fst, fstval := m.First()
	m = m.Remove(fst)
	assert.False(m.Has(fst))

	fst2, _ := m.First()
	assert.True(m.Has(fst2))

	m = m.Set(fst, fstval)
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
		assert.True(m.Type().Equals(MakeMapType(NumberType, NumberType)))

		m = m.Set(String("a"), String("a"))
		assert.Equal(m.Len(), uint64(n+1))
		assert.IsType(c, m.sequence())
		assert.True(m.Type().Equals(MakeMapType(MakeUnionType(NumberType, StringType), MakeUnionType(NumberType, StringType))))

		m = m.Remove(String("a"))
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.sequence())
		assert.True(m.Type().Equals(MakeMapType(NumberType, NumberType)))
	}

	test(10, mapLeafSequence{})
	test(1000, metaSequence{})
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
	for i := 1; !isMetaSequence(m.sequence()); i++ {
		k := Number(i)
		kvs = append(kvs, k, v)
		m = m.Set(k, v)
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
		m = m.Remove(k)
		assert.False(m.Has(k))
		assert.Equal(len(kvs)/2, int(m.Len()))
	}
}

func TestMapRemoveLastWhenNotLoaded(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := NewTestValueStore()
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
		nm = reload(nm.Remove(last.key))
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
