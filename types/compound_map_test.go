package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/stretchr/testify/assert"
)

type testMap struct {
	entries     []mapEntry
	less        testMapLessFn
	tr          Type
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

func (tm testMap) toCompoundMap(cs chunks.ChunkStore) compoundMap {
	keyvals := []Value{}
	for _, entry := range tm.entries {
		keyvals = append(keyvals, entry.key, entry.value)
	}
	return NewTypedMap(tm.tr, keyvals...).(compoundMap)
}

type testMapGenFn func(v Int64) Value

func newTestMap(length int, gen testMapGenFn, less testMapLessFn, tr Type) testMap {
	s := rand.NewSource(4242)
	used := map[int64]bool{}

	var mask int64 = 0xffffff
	entries := make([]mapEntry, 0, length)
	for len(entries) < length {
		v := s.Int63() & mask
		if _, ok := used[v]; !ok {
			entry := mapEntry{gen(Int64(v)), gen(Int64(v * 2))}
			entries = append(entries, entry)
			used[v] = true
		}
	}

	return testMap{entries, less, MakeCompoundType(MapKind, tr, tr), gen(Int64(mask + 1))}
}

func getTestNativeOrderMap(scale int) testMap {
	return newTestMap(int(mapPattern)*scale, func(v Int64) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, MakePrimitiveType(Int64Kind))
}

func getTestRefValueOrderMap(scale int) testMap {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	return newTestMap(int(mapPattern)*scale, func(v Int64) Value {
		return NewTypedSet(setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderMap(scale int) testMap {
	refType := MakeCompoundType(RefKind, MakePrimitiveType(Int64Kind))
	return newTestMap(int(mapPattern)*scale, func(v Int64) Value {
		return newRef(v.Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func getTestRefToValueOrderMap(scale int) testMap {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	refType := MakeCompoundType(RefKind, setType)
	return newTestMap(int(mapPattern)*scale, func(v Int64) Value {
		return newRef(NewTypedSet(setType, v).Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func TestCompoundMapHas(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		cs := chunks.NewMemoryStore()
		m := tm.toCompoundMap(cs)
		m2 := ReadValue(WriteValue(m, cs), cs).(compoundMap)
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
	doTest(getTestRefToNativeOrderMap(2))
	doTest(getTestRefToValueOrderMap(2))
}

func TestCompoundMapFirst(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap(chunks.NewMemoryStore())
		sort.Stable(tm)
		actualKey, actualValue := m.First()
		assert.True(tm.entries[0].key.Equals(actualKey))
		assert.True(tm.entries[0].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap(16))
	doTest(getTestRefValueOrderMap(2))
	doTest(getTestRefToNativeOrderMap(2))
	doTest(getTestRefToValueOrderMap(2))
}

func TestCompoundMapMaybeGet(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap(chunks.NewMemoryStore())
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
	doTest(getTestRefToNativeOrderMap(2))
	doTest(getTestRefToValueOrderMap(2))
}

func TestCompoundMapIter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap(chunks.NewMemoryStore())
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
	doTest(getTestRefToNativeOrderMap(2))
	doTest(getTestRefToValueOrderMap(2))
}

func TestCompoundMapIterAll(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap(chunks.NewMemoryStore())
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
	doTest(getTestRefToNativeOrderMap(2))
	doTest(getTestRefToValueOrderMap(2))
}

func TestCompoundMapSet(t *testing.T) {
	assert := assert.New(t)

	doTest := func(incr, offset int, tm testMap) {
		cs := chunks.NewMemoryStore()
		expected := tm.toCompoundMap(cs)
		run := func(from, to int) {
			actual := tm.Remove(from, to).toCompoundMap(cs).SetM(tm.Flatten(from, to)...)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
		}
		for i := 0; i < len(tm.entries)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(tm.entries)-offset, len(tm.entries))
		assert.Panics(func() {
			expected.Set(Int8(1), Bool(true))
		}, "Should panic due to wrong type")
	}

	doTest(18, 3, getTestNativeOrderMap(9))
	doTest(128, 1, getTestNativeOrderMap(32))
	doTest(64, 1, getTestRefValueOrderMap(4))
	doTest(64, 1, getTestRefToNativeOrderMap(4))
	doTest(64, 1, getTestRefToValueOrderMap(4))
}

func TestCompoundMapSetExistingKeyToExistingValue(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	tm := getTestNativeOrderMap(2)
	original := tm.toCompoundMap(cs)

	actual := original
	for _, entry := range tm.entries {
		actual = actual.Set(entry.key, entry.value).(compoundMap)
	}

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundMapSetExistingKeyToNewValue(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	tm := getTestNativeOrderMap(2)
	original := tm.toCompoundMap(cs)

	expectedWorking := tm
	actual := original
	for i, entry := range tm.entries {
		newValue := Int64(int64(entry.value.(Int64)) + 1)
		expectedWorking = expectedWorking.SetValue(i, newValue)
		actual = actual.Set(entry.key, newValue).(compoundMap)
	}

	expected := expectedWorking.toCompoundMap(cs)
	assert.Equal(expected.Len(), actual.Len())
	assert.True(expected.Equals(actual))
	assert.False(original.Equals(actual))
}

func TestCompoundMapRemove(t *testing.T) {
	assert := assert.New(t)

	doTest := func(incr int, tm testMap) {
		cs := chunks.NewMemoryStore()
		whole := tm.toCompoundMap(cs)
		run := func(i int) {
			expected := tm.Remove(i, i+1).toCompoundMap(cs)
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
	doTest(64, getTestRefToNativeOrderMap(4))
	doTest(64, getTestRefToValueOrderMap(4))
}

func TestCompoundMapRemoveNonexistentKey(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()
	tm := getTestNativeOrderMap(2)
	original := tm.toCompoundMap(cs)
	actual := original.Remove(Int64(-1)) // rand.Int63 returns non-negative numbers.

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestCompoundMapFilter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		cs := chunks.NewMemoryStore()
		m := tm.toCompoundMap(cs)
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
	doTest(getTestRefToNativeOrderMap(2))
	doTest(getTestRefToValueOrderMap(2))
}

func TestCompoundMapFirstNNumbers(t *testing.T) {
	assert := assert.New(t)

	mapType := MakeCompoundType(MapKind, MakePrimitiveType(Int64Kind), MakePrimitiveType(Int64Kind))

	kvs := []Value{}
	n := 5000
	for i := 0; i < n; i++ {
		kvs = append(kvs, Int64(i), Int64(i+1))
	}

	m := NewTypedMap(mapType, kvs...)
	assert.Equal(m.Ref().String(), "sha1-1b9664e55091370996f3af428ffee78f1ad36426")
}
