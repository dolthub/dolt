package types

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const testMapSize = 1000

type testMapGenFn func(v Number) Value

type testMap struct {
	entries     []mapEntry
	tr          *Type
	knownBadKey Value
}

func (tm testMap) Len() int {
	return len(tm.entries)
}

func (tm testMap) Less(i, j int) bool {
	return tm.entries[i].key.Less(tm.entries[j].key)
}

func (tm testMap) Swap(i, j int) {
	tm.entries[i], tm.entries[j] = tm.entries[j], tm.entries[i]
}

func (tm testMap) SetValue(i int, v Value) testMap {
	entries := make([]mapEntry, 0, len(tm.entries))
	entries = append(entries, tm.entries...)
	entries[i].value = v
	return testMap{entries, tm.tr, tm.knownBadKey}
}

func (tm testMap) Remove(from, to int) testMap {
	entries := make([]mapEntry, 0, len(tm.entries)-(to-from))
	entries = append(entries, tm.entries[:from]...)
	entries = append(entries, tm.entries[to:]...)
	return testMap{entries, tm.tr, tm.knownBadKey}
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
	added = make([]Value, 0)
	removed = make([]Value, 0)
	modified = make([]Value, 0)

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

func newTestMap(length int) testMap {
	entries := make([]mapEntry, 0, length)
	for i := 0; i < length; i++ {
		entry := mapEntry{Number(i), Number(i * 2)}
		entries = append(entries, entry)
	}
	return testMap{entries, MakeMapType(NumberType, NumberType), Number(length + 2)}
}

func newTestMapFromMap(m Map) testMap {
	entries := make([]mapEntry, 0, m.Len())
	m.IterAll(func(key, value Value) {
		entries = append(entries, mapEntry{key, value})
	})
	return testMap{entries, m.Type(), Number(-0)}
}

func newTestMapWithGen(length int, gen testMapGenFn, tr *Type) testMap {
	s := rand.NewSource(4242)
	used := map[int64]bool{}

	var mask int64 = 0xffffff
	entries := make([]mapEntry, 0, length)
	for len(entries) < length {
		v := s.Int63() & mask
		if _, ok := used[v]; !ok {
			entry := mapEntry{gen(Number(v)), gen(Number(v * 2))}
			entries = append(entries, entry)
			used[v] = true
		}
	}

	return testMap{entries, MakeMapType(tr, tr), gen(Number(mask + 1))}
}

type mapTestSuite struct {
	collectionTestSuite
	elems testMap
}

func newMapTestSuite(size uint, expectRefStr string, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int) *mapTestSuite {
	length := 1 << size
	elems := newTestMap(length)
	tr := MakeMapType(NumberType, NumberType)
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
				out := []Value{}
				l2.IterAll(func(key, value Value) {
					out = append(out, key)
					out = append(out, value)
				})
				return valueSlicesEqual(elems.FlattenAll(), out)
			},
			prependOne: func() Collection {
				dup := make([]mapEntry, length+1)
				dup[0] = mapEntry{Number(-1), Number(-2)}
				copy(dup[1:], elems.entries)
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key)
					flat = append(flat, entry.value)
				}
				return NewMap(flat...)
			},
			appendOne: func() Collection {
				dup := make([]mapEntry, length+1)
				copy(dup, elems.entries)
				dup[len(dup)-1] = mapEntry{Number(length*2 + 1), Number((length*2 + 1) * 2)}
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key)
					flat = append(flat, entry.value)
				}
				return NewMap(flat...)
			},
		},
		elems: elems,
	}
}

func TestMapSuite1K(t *testing.T) {
	suite.Run(t, newMapTestSuite(10, "sha1-e3f51f615e77c327b17bdb6cc0683b6b566158ca", 16, 2, 2))
}

func TestMapSuite4K(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, "sha1-af5c67cd9716fad095ed7b1446c098d80a87d87f", 56, 2, 2))
}

func getTestNativeOrderMap(scale int) testMap {
	return newTestMapWithGen(int(mapPattern)*scale, func(v Number) Value {
		return v
	}, NumberType)
}

func getTestRefValueOrderMap(scale int) testMap {
	setType := MakeSetType(NumberType)
	return newTestMapWithGen(int(mapPattern)*scale, func(v Number) Value {
		return NewSet(v)
	}, setType)
}

func getTestRefToNativeOrderMap(scale int, vw ValueWriter) testMap {
	refType := MakeRefType(NumberType)
	return newTestMapWithGen(int(mapPattern)*scale, func(v Number) Value {
		return vw.WriteValue(v)
	}, refType)
}

func getTestRefToValueOrderMap(scale int, vw ValueWriter) testMap {
	setType := MakeSetType(NumberType)
	refType := MakeRefType(setType)
	return newTestMapWithGen(int(mapPattern)*scale, func(v Number) Value {
		return vw.WriteValue(NewSet(v))
	}, refType)
}

func diffMapTest(assert *assert.Assertions, m1 Map, m2 Map, numAddsExpected int, numRemovesExpected int, numModifiedExpected int) (added []Value, removed []Value, modified []Value) {
	added, removed, modified = m1.Diff(m2)
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
	testMap1 := newTestMapWithGen(int(mapPattern)*2, func(v Number) Value {
		return v
	}, NumberType)
	testMap2 := newTestMapWithGen(int(mapPattern)*2, func(v Number) Value {
		return v
	}, NumberType)
	testMapAdded, testMapRemoved, testMapModified := testMap1.Diff(testMap2)
	map1 := testMap1.toMap()
	map2 := testMap2.toMap()
	mapDiffAdded, mapDiffRemoved, mapDiffModified := map1.Diff(map2)
	assert.Equal(t, testMapAdded, mapDiffAdded, "testMap.diff != map.diff")
	assert.Equal(t, testMapRemoved, mapDiffRemoved, "testMap.diff != map.diff")
	assert.Equal(t, testMapModified, mapDiffModified, "testMap.diff != map.diff")
}

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	m := NewMap()
	assert.Equal(uint64(0), m.Len())
	m = NewMap(NewString("foo1"), NewString("bar1"), NewString("foo2"), NewString("bar2"))
	assert.Equal(uint64(2), m.Len())
	assert.True(NewString("bar1").Equals(m.Get(NewString("foo1"))))
	assert.True(NewString("bar2").Equals(m.Get(NewString("foo2"))))
}

func TestMapUniqueKeysString(t *testing.T) {
	assert := assert.New(t)
	l := []Value{
		NewString("hello"), NewString("world"),
		NewString("foo"), NewString("bar"),
		NewString("bar"), NewString("foo"),
		NewString("hello"), NewString("foo"),
	}
	m := NewMap(l...)
	assert.Equal(uint64(3), m.Len())
	assert.True(NewString("foo").Equals(m.Get(NewString("hello"))))
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
	assert := assert.New(t)

	vs := NewTestValueStore()
	doTest := func(tm testMap) {
		m := tm.toMap()
		m2 := vs.ReadValue(vs.WriteValue(m).TargetRef()).(Map)
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
	assert.False(m1.Has(NewString("foo")))
	m2 := m1.Set(NewString("foo"), NewString("foo"))
	assert.False(m1.Has(NewString("foo")))
	assert.True(m2.Has(NewString("foo")))
	m3 := m1.Remove(NewString("foo"))
	assert.False(m1.Has(NewString("foo")))
	assert.True(m2.Has(NewString("foo")))
	assert.False(m3.Has(NewString("foo")))
}

func TestMapRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
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

	tm := getTestNativeOrderMap(2)
	original := tm.toMap()
	actual := original.Remove(Number(-1)) // rand.Int63 returns non-negative numbers.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestMapFirst(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	k, v := m1.First()
	assert.Nil(k)
	assert.Nil(v)

	m1 = m1.Set(NewString("foo"), NewString("bar"))
	m1 = m1.Set(NewString("hot"), NewString("dog"))
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

	doTest := func(tm testMap) {
		m := tm.toMap()
		sort.Stable(tm)
		actualKey, actualValue := m.First()
		assert.True(tm.entries[0].key.Equals(actualKey))
		assert.True(tm.entries[0].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, NewTestValueStore()))
}

func TestMapSetGet(t *testing.T) {
	assert := assert.New(t)
	m1 := NewMap()
	assert.Nil(m1.Get(NewString("foo")))
	m2 := m1.Set(NewString("foo"), Number(42))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Number(42).Equals(m2.Get(NewString("foo"))))
	m3 := m2.Set(NewString("foo"), Number(43))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Number(42).Equals(m2.Get(NewString("foo"))))
	assert.True(Number(43).Equals(m3.Get(NewString("foo"))))
	m4 := m3.Remove(NewString("foo"))
	assert.Nil(m1.Get(NewString("foo")))
	assert.True(Number(42).Equals(m2.Get(NewString("foo"))))
	assert.True(Number(43).Equals(m3.Get(NewString("foo"))))
	assert.Nil(m4.Get(NewString("foo")))
}

func TestMapSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
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
	m3 := m2.SetM(NewString("foo"), NewString("bar"), NewString("hot"), NewString("dog"))
	assert.Equal(uint64(2), m3.Len())
	assert.True(NewString("bar").Equals(m3.Get(NewString("foo"))))
	assert.True(NewString("dog").Equals(m3.Get(NewString("hot"))))
	m4 := m3.SetM(NewString("mon"), NewString("key"))
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

	m = m.SetM(NewString("a"), Number(0), NewString("b"), Number(1))
	m.Iter(cb)
	assert.Equal(2, len(results))
	assert.True(got(NewString("a"), Number(0)))
	assert.True(got(NewString("b"), Number(1)))

	results = resultList{}
	stop = true
	m.Iter(cb)
	assert.Equal(1, len(results))
	// Iteration order not guaranteed, but it has to be one of these.
	assert.True(got(NewString("a"), Number(0)) || got(NewString("b"), Number(1)))
}

func TestMapIter2(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toMap()
		sort.Sort(tm)
		idx := uint64(0)
		endAt := uint64(mapPattern)

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
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toMap()
		sort.Sort(tm)
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

	m1 = NewMap(NewString("foo"), Number(0.0), NewString("bar"), NewList())
	m2 = m2.SetM(NewString("foo"), Number(0.0), NewString("bar"), NewList())
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
		Bool(true), NewString("true"),
		Bool(false), NewString("false"),
		Number(1), NewString("Number: 1"),
		Number(0), NewString("Number: 0"),
		b1, NewString("blob1"),
		b2, NewString("blob2"),
		NewList(), NewString("empty list"),
		NewList(NewList()), NewString("list of list"),
		NewMap(), NewString("empty map"),
		NewMap(NewMap(), NewMap()), NewString("map of map/map"),
		NewSet(), NewString("empty set"),
		NewSet(NewSet()), NewString("map of set/set"),
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
		assert.Equal(expectOrdering[i].Ref().String(), key.Ref().String())
		i++
	})
}

func TestMapOrdering(t *testing.T) {
	assert := assert.New(t)

	testMapOrder(assert,
		StringType, StringType,
		[]Value{
			NewString("a"), NewString("unused"),
			NewString("z"), NewString("unused"),
			NewString("b"), NewString("unused"),
			NewString("y"), NewString("unused"),
			NewString("c"), NewString("unused"),
			NewString("x"), NewString("unused"),
		},
		[]Value{
			NewString("a"),
			NewString("b"),
			NewString("c"),
			NewString("x"),
			NewString("y"),
			NewString("z"),
		},
	)

	testMapOrder(assert,
		NumberType, StringType,
		[]Value{
			Number(0), NewString("unused"),
			Number(1000), NewString("unused"),
			Number(1), NewString("unused"),
			Number(100), NewString("unused"),
			Number(2), NewString("unused"),
			Number(10), NewString("unused"),
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
			Number(0), NewString("unused"),
			Number(-30), NewString("unused"),
			Number(25), NewString("unused"),
			Number(1002), NewString("unused"),
			Number(-5050), NewString("unused"),
			Number(23), NewString("unused"),
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
			Number(0.0001), NewString("unused"),
			Number(0.000001), NewString("unused"),
			Number(1), NewString("unused"),
			Number(25.01e3), NewString("unused"),
			Number(-32.231123e5), NewString("unused"),
			Number(23), NewString("unused"),
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
			NewString("a"), NewString("unused"),
			NewString("z"), NewString("unused"),
			NewString("b"), NewString("unused"),
			NewString("y"), NewString("unused"),
			NewString("c"), NewString("unused"),
			NewString("x"), NewString("unused"),
		},
		[]Value{
			NewString("a"),
			NewString("b"),
			NewString("c"),
			NewString("x"),
			NewString("y"),
			NewString("z"),
		},
	)

	testMapOrder(assert,
		BoolType, StringType,
		[]Value{
			Bool(true), NewString("unused"),
			Bool(false), NewString("unused"),
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
	m = m.Set(Bool(false), NewString("hi"))
	assert.False(m.Empty())
	m = m.Set(NewList(), NewMap())
	assert.False(m.Empty())
}

func TestMapType(t *testing.T) {
	assert := assert.New(t)

	emptyMapType := MakeMapType(MakeUnionType(), MakeUnionType())
	m := NewMap()
	assert.True(m.Type().Equals(emptyMapType))

	m2 := m.Remove(NewString("B"))
	assert.True(emptyMapType.Equals(m2.Type()))

	tr := MakeMapType(StringType, NumberType)
	m2 = m.Set(NewString("A"), Number(1))
	assert.True(tr.Equals(m2.Type()))

	m2 = m.SetM(NewString("B"), Number(2), NewString("C"), Number(2))
	assert.True(tr.Equals(m2.Type()))

	m3 := m2.Set(NewString("A"), Bool(true))
	assert.True(MakeMapType(StringType, MakeUnionType(BoolType, NumberType)).Equals(m3.Type()), m3.Type().Describe())
	m4 := m3.Set(Bool(true), Number(1))
	assert.True(MakeMapType(MakeUnionType(BoolType, StringType), MakeUnionType(BoolType, NumberType)).Equals(m4.Type()))
}

func TestMapChunks(t *testing.T) {
	assert := assert.New(t)

	l1 := NewMap(Number(0), Number(1))
	c1 := l1.Chunks()
	assert.Len(c1, 0)

	l2 := NewMap(NewRef(Number(0)), Number(1))
	c2 := l2.Chunks()
	assert.Len(c2, 1)

	l3 := NewMap(Number(0), NewRef(Number(1)))
	c3 := l3.Chunks()
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
	assert.Equal("sha1-2bc451349d04c5f90cfe73d1e6eb3ee626db99a1", m.Ref().String())
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
		k := vs.WriteValue(NewStruct("num", structData{"n": Number(i)}))
		v := vs.WriteValue(NewStruct("num", structData{"n": Number(i + 1)}))
		assert.NotNil(k)
		assert.NotNil(v)
		kvs = append(kvs, k, v)
	}

	m := NewMap(kvs...)
	assert.Equal("sha1-5c9a17f6da0ebfebc1f82f498ac46992fad85250", m.Ref().String())
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(deriveCollectionHeight(m)+1, getRefHeightOfCollection(m))
}

func TestMapModifyAfterRead(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()
	m := getTestNativeOrderMap(2).toMap()
	// Drop chunk values.
	m = vs.ReadValue(vs.WriteValue(m).TargetRef()).(Map)
	// Modify/query. Once upon a time this would crash.
	fst, fstval := m.First()
	m = m.Remove(fst)
	assert.False(m.Has(fst))

	fst2, _ := m.First()
	assert.True(m.Has(fst2))

	m = m.Set(fst, fstval)
	assert.True(m.Has(fst))
}
