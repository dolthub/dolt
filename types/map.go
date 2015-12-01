package types

import (
	"runtime"
	"sort"
	"sync"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type Map struct {
	data    mapData // sorted by entry.key.Ref()
	indexOf indexOfMapFn
	t       Type
	ref     *ref.Ref
	cs      chunks.ChunkStore
}

type mapData []mapEntry

type indexOfMapFn func(m mapData, v Value) int

func NewMap(cs chunks.ChunkStore, kv ...Value) Map {
	return NewTypedMap(cs, mapType, kv...)
}

func NewTypedMap(cs chunks.ChunkStore, t Type, kv ...Value) Map {
	return newMapFromData(cs, buildMapData(mapData{}, kv, t), t)
}

func (m Map) First() (Value, Value) {
	if len(m.data) == 0 {
		return nil, nil
	} else {
		entry := m.data[0]
		return entry.key, entry.value
	}
}

func (m Map) Len() uint64 {
	return uint64(len(m.data))
}

func (m Map) Empty() bool {
	return m.Len() == uint64(0)
}

func (m Map) Has(key Value) bool {
	idx := m.indexOf(m.data, key)
	return idx < len(m.data) && m.data[idx].key.Equals(key)
}

func (m Map) Get(key Value) Value {
	if v, ok := m.MaybeGet(key); ok {
		return v
	}
	return nil
}

func (m Map) MaybeGet(key Value) (v Value, ok bool) {
	idx := m.indexOf(m.data, key)
	if idx < len(m.data) {
		entry := m.data[idx]
		if entry.key.Equals(key) {
			return entry.value, true
		}
	}
	return
}

func (m Map) Set(key Value, val Value) Map {
	elemTypes := m.t.Desc.(CompoundDesc).ElemTypes
	assertType(elemTypes[0], key)
	assertType(elemTypes[1], val)
	return newMapFromData(m.cs, buildMapData(m.data, []Value{key, val}, m.t), m.t)
}

func (m Map) SetM(kv ...Value) Map {
	assertMapElemTypes(m, kv...)
	return newMapFromData(m.cs, buildMapData(m.data, kv, m.t), m.t)
}

func (m Map) Remove(k Value) Map {
	idx := m.indexOf(m.data, k)
	if idx == len(m.data) || !m.data[idx].key.Equals(k) {
		return m
	}

	data := make(mapData, len(m.data)-1)
	copy(data, m.data[:idx])
	copy(data[idx:], m.data[idx+1:])
	return newMapFromData(m.cs, data, m.t)
}

type mapIterCallback func(key, value Value) (stop bool)

func (m Map) Iter(cb mapIterCallback) {
	for _, entry := range m.data {
		if cb(entry.key, entry.value) {
			break
		}
	}
}

type mapIterAllCallback func(key, value Value)

func (m Map) IterAll(cb mapIterAllCallback) {
	for _, entry := range m.data {
		cb(entry.key, entry.value)
	}
}

func (m Map) IterAllP(concurrency int, f mapIterAllCallback) {
	if concurrency == 0 {
		concurrency = runtime.NumCPU()
	}
	sem := make(chan int, concurrency)

	wg := sync.WaitGroup{}

	for idx := range m.data {
		wg.Add(1)

		sem <- 1
		go func(idx int) {
			defer wg.Done()
			md := m.data[idx]
			f(md.key, md.value)
			<-sem
		}(idx)
	}

	wg.Wait()
}

type mapFilterCallback func(key, value Value) (keep bool)

func (m Map) Filter(cb mapFilterCallback) Map {
	data := mapData{}
	for _, entry := range m.data {
		if cb(entry.key, entry.value) {
			data = append(data, entry)
		}
	}
	// Already sorted.
	return newMapFromData(m.cs, data, m.t)
}

func (m Map) Ref() ref.Ref {
	return EnsureRef(m.ref, m)
}

func (m Map) Equals(other Value) bool {
	return other != nil && m.t.Equals(other.Type()) && m.Ref() == other.Ref()
}

func (m Map) Chunks() (chunks []ref.Ref) {
	for _, entry := range m.data {
		chunks = append(chunks, entry.key.Chunks()...)
		chunks = append(chunks, entry.value.Chunks()...)
	}
	return
}

func (m Map) ChildValues() []Value {
	res := make([]Value, 2*len(m.data))
	for i, entry := range m.data {
		res[i*2] = entry.key
		res[i*2+1] = entry.value
	}
	return res
}

var mapType = MakeCompoundType(MapKind, MakePrimitiveType(ValueKind), MakePrimitiveType(ValueKind))

func (m Map) Type() Type {
	return m.t
}

func (m Map) elemTypes() []Type {
	return m.t.Desc.(CompoundDesc).ElemTypes
}

type mapEntry struct {
	key   Value
	value Value
}

func newMapFromData(cs chunks.ChunkStore, data mapData, t Type) Map {
	return Map{data, getIndexFnForMapType(t), t, &ref.Ref{}, cs}
}

func buildMapData(oldData mapData, values []Value, t Type) mapData {
	idxFn := getIndexFnForMapType(t)
	elemTypes := t.Desc.(CompoundDesc).ElemTypes

	// Sadly, d.Chk.Equals() costs too much. BUG #83
	d.Chk.True(0 == len(values)%2, "Must specify even number of key/value pairs")

	data := make(mapData, len(oldData), len(oldData)+len(values))
	copy(data, oldData)
	for i := 0; i < len(values); i += 2 {
		k := values[i]
		v := values[i+1]
		assertType(elemTypes[0], k)
		assertType(elemTypes[1], v)
		idx := idxFn(data, k)
		if idx < len(data) && data[idx].key.Equals(k) {
			if !data[idx].value.Equals(v) {
				data[idx] = mapEntry{k, v}
			}
			continue
		}

		// TODO: These repeated copies suck. We're not allocating more memory (because we made the slice with the correct capacity to begin with above - yay!), but still, this is more work than necessary. Perhaps we should use an actual BST for the in-memory state, rather than a flat list.
		data = append(data, mapEntry{})
		copy(data[idx+1:], data[idx:])
		data[idx] = mapEntry{k, v}
	}
	return data
}

func getIndexFnForMapType(t Type) indexOfMapFn {
	orderByValue := t.Desc.(CompoundDesc).ElemTypes[0].IsOrdered()
	if orderByValue {
		return indexOfOrderedMapValue
	}

	return indexOfMapValue
}

func indexOfMapValue(m mapData, v Value) int {
	return sort.Search(len(m), func(i int) bool {
		return !m[i].key.Ref().Less(v.Ref())
	})
}

func indexOfOrderedMapValue(m mapData, v Value) int {
	ov := v.(OrderedValue)

	return sort.Search(len(m), func(i int) bool {
		return !m[i].key.(OrderedValue).Less(ov)
	})
}
