package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testMapSize = 1000

type testMap struct {
	entries     []mapEntry
	less        testMapLessFn
	tr          *Type
	knownBadKey Value
}

type testMapLessFn func(x, y Value) bool

func (tm testMap) Len() int {
	return len(tm.entries)
}

func (tm testMap) Less(i, j int) bool {
	return tm.less(tm.entries[i].key, tm.entries[j].key)
}

func (tm testMap) Swap(i, j int) {
	tm.entries[i], tm.entries[j] = tm.entries[j], tm.entries[i]
}

func (tm testMap) SetValue(i int, v Value) testMap {
	entries := make([]mapEntry, 0, len(tm.entries))
	entries = append(entries, tm.entries...)
	entries[i].value = v
	return testMap{entries, tm.less, tm.tr, tm.knownBadKey}
}

func (tm testMap) Remove(from, to int) testMap {
	entries := make([]mapEntry, 0, len(tm.entries)-(to-from))
	entries = append(entries, tm.entries[:from]...)
	entries = append(entries, tm.entries[to:]...)
	return testMap{entries, tm.less, tm.tr, tm.knownBadKey}
}

func (tm testMap) Flatten(from, to int) []Value {
	flat := make([]Value, 0, len(tm.entries)*2)
	for _, entry := range tm.entries[from:to] {
		flat = append(flat, entry.key)
		flat = append(flat, entry.value)
	}
	return flat
}

func (tm testMap) toCompoundMap() compoundMap {
	keyvals := []Value{}
	for _, entry := range tm.entries {
		keyvals = append(keyvals, entry.key, entry.value)
	}
	return NewTypedMap(tm.tr, keyvals...).(compoundMap)
}

type testMapGenFn func(v Number) Value

func newTestMap(length int, gen testMapGenFn, less testMapLessFn, tr *Type) testMap {
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

	return testMap{entries, less, MakeMapType(tr, tr), gen(Number(mask + 1))}
}

func getTestNativeOrderMap(scale int) testMap {
	return newTestMap(int(mapPattern)*scale, func(v Number) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, NumberType)
}

func getTestRefValueOrderMap(scale int) testMap {
	setType := MakeSetType(NumberType)
	return newTestMap(int(mapPattern)*scale, func(v Number) Value {
		return NewTypedSet(setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderMap(scale int, vw ValueWriter) testMap {
	refType := MakeRefType(NumberType)
	return newTestMap(int(mapPattern)*scale, func(v Number) Value {
		return vw.WriteValue(v)
	}, func(x, y Value) bool {
		return !y.(Ref).TargetRef().Less(x.(Ref).TargetRef())
	}, refType)
}

func getTestRefToValueOrderMap(scale int, vw ValueWriter) testMap {
	setType := MakeSetType(NumberType)
	refType := MakeRefType(setType)
	return newTestMap(int(mapPattern)*scale, func(v Number) Value {
		return vw.WriteValue(NewTypedSet(setType, v))
	}, func(x, y Value) bool {
		return !y.(Ref).TargetRef().Less(x.(Ref).TargetRef())
	}, refType)
}

func TestCompoundMapHas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	vs := NewTestValueStore()
	doTest := func(tm testMap) {
		m := tm.toCompoundMap()
		m2 := vs.ReadValue(vs.WriteValue(m).TargetRef()).(compoundMap)
		for _, entry := range tm.entries {
			k, v := entry.key, entry.value
			assert.True(m.Has(k))
			assert.True(m.Get(k).Equals(v))
			assert.True(m2.Has(k))
			assert.True(m2.Get(k).Equals(v))
		}
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, vs))
	doTest(getTestRefToValueOrderMap(2, vs))
}

func TestCompoundMapFirst(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap()
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

func TestCompoundMapMaybeGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap()
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

func TestCompoundMapIter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap()
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

func TestCompoundMapIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap()
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

func TestCompoundMapSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(incr, offset int, tm testMap) {
		expected := tm.toCompoundMap()
		run := func(from, to int) {
			actual := tm.Remove(from, to).toCompoundMap().SetM(tm.Flatten(from, to)...)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
		for i := 0; i < len(tm.entries)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(tm.entries)-offset, len(tm.entries))
		assert.Panics(func() {
			expected.Set(Number(1), Bool(true))
		}, "Should panic due to wrong type")
	}

	doTest(18, 3, getTestNativeOrderMap(9))
	doTest(128, 1, getTestNativeOrderMap(32))
	doTest(64, 1, getTestRefValueOrderMap(4))
	doTest(64, 1, getTestRefToNativeOrderMap(4, NewTestValueStore()))
	doTest(64, 1, getTestRefToValueOrderMap(4, NewTestValueStore()))
}

func TestCompoundMapSetExistingKeyToExistingValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	tm := getTestNativeOrderMap(2)
	original := tm.toCompoundMap()

	actual := original
	for _, entry := range tm.entries {
		actual = actual.Set(entry.key, entry.value).(compoundMap)
	}

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundMapSetExistingKeyToNewValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	tm := getTestNativeOrderMap(2)
	original := tm.toCompoundMap()

	expectedWorking := tm
	actual := original
	for i, entry := range tm.entries {
		newValue := Number(int64(entry.value.(Number)) + 1)
		expectedWorking = expectedWorking.SetValue(i, newValue)
		actual = actual.Set(entry.key, newValue).(compoundMap)
	}

	expected := expectedWorking.toCompoundMap()
	assert.Equal(expected.Len(), actual.Len())
	assert.True(expected.Equals(actual))
	assert.False(original.Equals(actual))
}

func TestCompoundMapRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(incr int, tm testMap) {
		whole := tm.toCompoundMap()
		run := func(i int) {
			expected := tm.Remove(i, i+1).toCompoundMap()
			actual := whole.Remove(tm.entries[i].key)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
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

func TestCompoundMapRemoveNonexistentKey(t *testing.T) {
	assert := assert.New(t)

	tm := getTestNativeOrderMap(2)
	original := tm.toCompoundMap()
	actual := original.Remove(Number(-1)) // rand.Int63 returns non-negative numbers.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundMapFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap()
		sort.Sort(tm)
		pivotPoint := 10
		pivot := tm.entries[pivotPoint].key
		actual := m.Filter(func(k, v Value) bool {
			return tm.less(k, pivot)
		})
		assert.True(newTypedMap(tm.tr, tm.entries[:pivotPoint+1]...).Equals(actual))

		idx := 0
		actual.IterAll(func(k, v Value) {
			assert.True(tm.entries[idx].key.Equals(k), "%v != %v", k, tm.entries[idx].key)
			assert.True(tm.entries[idx].value.Equals(v), "%v != %v", v, tm.entries[idx].value)
			idx++
		})
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2, NewTestValueStore()))
	doTest(getTestRefToValueOrderMap(2, NewTestValueStore()))
}

func TestCompoundMapFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	mapType := MakeMapType(NumberType, NumberType)

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		kvs = append(kvs, Number(i), Number(i+1))
	}

	m := NewTypedMap(mapType, kvs...).(compoundMap)
	assert.Equal("sha1-2bc451349d04c5f90cfe73d1e6eb3ee626db99a1", m.Ref().String())
	height := deriveCompoundMapHeight(m)
	assert.Equal(height, m.tuples[0].childRef.Height())
}

func TestCompoundMapRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)
	vs := NewTestValueStore()

	structType := MakeStructType("num", TypeMap{
		"n": NumberType,
	})
	refOfTypeStructType := MakeRefType(structType)

	mapType := MakeMapType(refOfTypeStructType, refOfTypeStructType)

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		k := vs.WriteValue(NewStruct(structType, structData{"n": Number(i)}))
		v := vs.WriteValue(NewStruct(structType, structData{"n": Number(i + 1)}))
		assert.NotNil(k)
		assert.NotNil(v)
		kvs = append(kvs, k, v)
	}

	m := NewTypedMap(mapType, kvs...).(compoundMap)
	assert.Equal("sha1-3d8eea119bc685942107f7b9513b33d2f763d693", m.Ref().String())
	height := deriveCompoundMapHeight(m)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(height+1, m.tuples[0].childRef.Height())
}

func TestCompoundMapModifyAfterRead(t *testing.T) {
	assert := assert.New(t)
	vs := NewTestValueStore()
	m := getTestNativeOrderMap(2).toCompoundMap()
	// Drop chunk values.
	m = vs.ReadValue(vs.WriteValue(m).TargetRef()).(compoundMap)
	// Modify/query. Once upon a time this would crash.
	fst, fstval := m.First()
	m = m.Remove(fst).(compoundMap)
	assert.False(m.Has(fst))
	{
		fst, _ := m.First()
		assert.True(m.Has(fst))
	}
	m = m.Set(fst, fstval).(compoundMap)
	assert.True(m.Has(fst))
}

func deriveCompoundMapHeight(m compoundMap) uint64 {
	// Note: not using mt.childRef.Height() because the purpose of this method is to be redundant.
	height := uint64(1)
	if m2, ok := m.getItem(0).(metaTuple).child.(compoundMap); ok {
		height += deriveCompoundMapHeight(m2)
	}
	return height
}
