package types

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/chunks"
)

type testNativeOrderMap []mapEntry

func (tm testNativeOrderMap) Len() int {
	return len(tm)
}

func (tm testNativeOrderMap) Less(i, j int) bool {
	return tm[i].key.(OrderedValue).Less(tm[j].key.(OrderedValue))
}

func (tm testNativeOrderMap) Swap(i, j int) {
	tm[i], tm[j] = tm[j], tm[i]
}

func getTestNativeOrderMap() ([]mapEntry, []Value) {
	length := int(mapPattern * 16)
	s := rand.NewSource(42)
	used := map[int64]bool{}

	mapData := testNativeOrderMap{}
	values := []Value{}

	for len(values) < length {
		v := s.Int63() & 0xffffff
		if _, ok := used[v]; !ok {
			entry := mapEntry{Int64(v), Int64(v * 2)}
			mapData = append(mapData, entry)
			used[v] = true
			values = append(values, entry.key, entry.value)
		}
	}

	sort.Sort(mapData)
	return mapData, values
}

type testRefOrderMap []mapEntry

func (tm testRefOrderMap) Len() int {
	return len(tm)
}

func (tm testRefOrderMap) Less(i, j int) bool {
	return tm[i].key.Ref().Less(tm[j].key.Ref())
}

func (tm testRefOrderMap) Swap(i, j int) {
	tm[i], tm[j] = tm[j], tm[i]
}

func getTestRefOrderMap() ([]mapEntry, []Value) {
	length := int(mapPattern * 16)
	s := rand.NewSource(42)
	used := map[int64]bool{}

	values := []Value{}
	mapData := testRefOrderMap{}
	for i := 0; i < length; i++ {
		v := s.Int63() & 0xffffff
		if _, ok := used[v]; !ok {
			entry := mapEntry{NewRef(Int64(v).Ref()), Int64(v)}
			mapData = append(mapData, entry)
			used[v] = true
			values = append(values, entry.key, entry.value)
		}
	}

	sort.Sort(mapData)
	return mapData, values
}

func TestCompoundMapHasAndGet(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	doTest := func(simpleMap []mapEntry, m compoundMap) {
		for _, v := range simpleMap {
			assert.True(m.Has(v.key))
			assert.True(m.Get(v.key).Equals(v.value))
		}
	}

	simpleMap, kv := getTestNativeOrderMap()
	tr := MakeCompoundType(MapKind, MakePrimitiveType(Int64Kind), MakePrimitiveType(Int64Kind))
	m := NewTypedMap(cs, tr, kv...).(compoundMap)
	doTest(simpleMap, m)

	simpleMap, kv = getTestRefOrderMap()
	tr = MakeCompoundType(MapKind, MakeCompoundType(RefKind, MakePrimitiveType(ValueKind)), MakePrimitiveType(Int64Kind))
	m = NewTypedMap(cs, tr, kv...).(compoundMap)
	doTest(simpleMap, m)
}

func TestCompoundMapIter(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	doTest := func(simpleMap []mapEntry, m compoundMap) {
		idx := uint64(0)
		endAt := uint64(mapPattern)

		m.Iter(func(k, v Value) bool {
			assert.True(simpleMap[idx].key.Equals(k))
			assert.True(simpleMap[idx].value.Equals(v))
			if idx == endAt {
				return true
			}
			idx++
			return false
		})

		assert.Equal(endAt, idx)
	}

	simpleMap, kv := getTestNativeOrderMap()
	tr := MakeCompoundType(MapKind, MakePrimitiveType(Int64Kind), MakePrimitiveType(Int64Kind))
	m := NewTypedMap(cs, tr, kv...).(compoundMap)
	doTest(simpleMap, m)

	simpleMap, kv = getTestRefOrderMap()
	tr = MakeCompoundType(MapKind, MakeCompoundType(RefKind, MakePrimitiveType(ValueKind)), MakePrimitiveType(Int64Kind))
	m = NewTypedMap(cs, tr, kv...).(compoundMap)
	doTest(simpleMap, m)
}

func TestCompoundMapIterAll(t *testing.T) {
	assert := assert.New(t)

	cs := chunks.NewMemoryStore()

	doTest := func(simpleMap []mapEntry, m compoundMap) {
		idx := uint64(0)
		m.IterAll(func(k, v Value) {
			assert.True(simpleMap[idx].key.Equals(k))
			assert.True(simpleMap[idx].value.Equals(v))
			idx++
		})
	}

	simpleMap, kv := getTestNativeOrderMap()
	tr := MakeCompoundType(MapKind, MakePrimitiveType(Int64Kind), MakePrimitiveType(Int64Kind))
	m := NewTypedMap(cs, tr, kv...).(compoundMap)
	doTest(simpleMap, m)

	simpleMap, kv = getTestRefOrderMap()
	tr = MakeCompoundType(MapKind, MakeCompoundType(RefKind, MakePrimitiveType(ValueKind)), MakePrimitiveType(Int64Kind))
	m = NewTypedMap(cs, tr, kv...).(compoundMap)
	doTest(simpleMap, m)
}
