package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testMap struct {
	entries []mapEntry
	less    testMapLessFn
	tr      Type
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

func (tm testMap) toCompoundMap(cs chunks.ChunkStore) compoundMap {
	keyvals := []Value{}
	for _, entry := range tm.entries {
		keyvals = append(keyvals, entry.key, entry.value)
	}
	return NewTypedMap(cs, tm.tr, keyvals...).(compoundMap)
}

type testMapGenFn func(v Int64) Value

func newTestMap(length int, gen testMapGenFn, less testMapLessFn, tr Type) testMap {
	s := rand.NewSource(4242)
	used := map[int64]bool{}

	var entries []mapEntry
	for len(entries) < length {
		v := s.Int63() & 0xffffff
		if _, ok := used[v]; !ok {
			entry := mapEntry{gen(Int64(v)), gen(Int64(v * 2))}
			entries = append(entries, entry)
			used[v] = true
		}
	}

	return testMap{entries, less, MakeCompoundType(MapKind, tr, tr)}
}

func getTestNativeOrderMap() testMap {
	return newTestMap(int(mapPattern*16), func(v Int64) Value {
		return v
	}, func(x, y Value) bool {
		return !y.(OrderedValue).Less(x.(OrderedValue))
	}, MakePrimitiveType(Int64Kind))
}

func getTestRefValueOrderMap() testMap {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	return newTestMap(int(mapPattern*2), func(v Int64) Value {
		return NewTypedSet(chunks.NewMemoryStore(), setType, v)
	}, func(x, y Value) bool {
		return !y.Ref().Less(x.Ref())
	}, setType)
}

func getTestRefToNativeOrderMap() testMap {
	refType := MakeCompoundType(RefKind, MakePrimitiveType(Int64Kind))
	return newTestMap(int(mapPattern*2), func(v Int64) Value {
		return newRef(v.Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func getTestRefToValueOrderMap() testMap {
	setType := MakeCompoundType(SetKind, MakePrimitiveType(Int64Kind))
	refType := MakeCompoundType(RefKind, setType)
	return newTestMap(int(mapPattern*2), func(v Int64) Value {
		return newRef(NewTypedSet(chunks.NewMemoryStore(), setType, v).Ref(), refType)
	}, func(x, y Value) bool {
		return !y.(RefBase).TargetRef().Less(x.(RefBase).TargetRef())
	}, refType)
}

func TestCompoundMapHas(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap(chunks.NewMemoryStore())
		for _, entry := range tm.entries {
			assert.True(m.Has(entry.key))
			assert.True(m.Get(entry.key).Equals(entry.value))
		}
	}

	doTest(getTestNativeOrderMap())
	doTest(getTestRefValueOrderMap())
	doTest(getTestRefToNativeOrderMap())
	doTest(getTestRefToValueOrderMap())
}

func TestCompoundMapFirst(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap(chunks.NewMemoryStore())
		sort.Sort(tm)
		actualKey, actualValue := m.First()
		assert.True(tm.entries[0].key.Equals(actualKey))
		assert.True(tm.entries[0].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap())
	doTest(getTestRefValueOrderMap())
	doTest(getTestRefToNativeOrderMap())
	doTest(getTestRefToValueOrderMap())
}

func TestCompoundMapIter(t *testing.T) {
	assert := assert.New(t)

	doTest := func(tm testMap) {
		m := tm.toCompoundMap(chunks.NewMemoryStore())
		sort.Sort(tm)
		idx := uint64(0)
		endAt := uint64(mapPattern)

		m.Iter(func(k, v Value) bool {
			assert.True(tm.entries[idx].key.Equals(k))
			assert.True(tm.entries[idx].value.Equals(v))
			if idx == endAt {
				idx += 1
				return true
			}

			idx += 1
			return false
		})

		assert.Equal(endAt, idx-1)
	}

	doTest(getTestNativeOrderMap())
	doTest(getTestRefValueOrderMap())
	doTest(getTestRefToNativeOrderMap())
	doTest(getTestRefToValueOrderMap())
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

	doTest(getTestNativeOrderMap())
	doTest(getTestRefValueOrderMap())
	doTest(getTestRefToNativeOrderMap())
	doTest(getTestRefToValueOrderMap())
}
