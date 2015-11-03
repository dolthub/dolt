package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type Map struct {
	data mapData // sorted by entry.key.Ref()
	t    TypeRef
	ref  *ref.Ref
}

type mapData []mapEntry

func NewMap(kv ...Value) Map {
	return newMapFromData(buildMapData(mapData{}, kv), mapTypeRef)
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
	idx := indexMapData(m.data, key.Ref())
	return idx < len(m.data) && m.data[idx].key.Equals(key)
}

func (m Map) Get(key Value) Value {
	if v, ok := m.MaybeGet(key); ok {
		return v
	}
	return nil
}

func (m Map) MaybeGet(key Value) (v Value, ok bool) {
	idx := indexMapData(m.data, key.Ref())
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
	return newMapFromData(buildMapData(m.data, []Value{key, val}), m.t)
}

func (m Map) SetM(kv ...Value) Map {
	assertMapElemTypes(m, kv...)
	return newMapFromData(buildMapData(m.data, kv), m.t)
}

func (m Map) Remove(k Value) Map {
	idx := indexMapData(m.data, k.Ref())
	if idx == len(m.data) || !m.data[idx].key.Equals(k) {
		return m
	}

	data := make(mapData, len(m.data)-1)
	copy(data, m.data[:idx])
	copy(data[idx:], m.data[idx+1:])
	return newMapFromData(data, m.t)
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

type mapFilterCallback func(key, value Value) (keep bool)

func (m Map) Filter(cb mapFilterCallback) Map {
	data := mapData{}
	for _, entry := range m.data {
		if cb(entry.key, entry.value) {
			data = append(data, entry)
		}
	}
	// Already sorted.
	return newMapFromData(data, m.t)
}

func (m Map) Ref() ref.Ref {
	return EnsureRef(m.ref, m)
}

func (m Map) Equals(other Value) bool {
	return other != nil && m.t.Equals(other.TypeRef()) && m.Ref() == other.Ref()
}

func (m Map) Chunks() (chunks []ref.Ref) {
	for _, entry := range m.data {
		chunks = append(chunks, entry.key.Chunks()...)
		chunks = append(chunks, entry.value.Chunks()...)
	}
	return
}

var mapTypeRef = MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(ValueKind), MakePrimitiveTypeRef(ValueKind))

func (m Map) TypeRef() TypeRef {
	return m.t
}

func (m Map) elemTypes() []TypeRef {
	return m.t.Desc.(CompoundDesc).ElemTypes
}

func init() {
	RegisterFromValFunction(mapTypeRef, func(v Value) Value {
		return v.(Map)
	})
}

type mapEntry struct {
	key   Value
	value Value
}

func newMapFromData(data mapData, t TypeRef) Map {
	return Map{data, t, &ref.Ref{}}
}

func buildMapData(oldData mapData, values []Value) mapData {
	// Sadly, d.Chk.Equals() costs too much. BUG #83
	d.Chk.True(0 == len(values)%2, "Must specify even number of key/value pairs")

	data := make(mapData, len(oldData), len(oldData)+len(values))
	copy(data, oldData)
	for i := 0; i < len(values); i += 2 {
		k := values[i]
		v := values[i+1]
		idx := indexMapData(data, k.Ref())
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

func indexMapData(m mapData, r ref.Ref) int {
	return sort.Search(len(m), func(i int) bool {
		return !ref.Less(m[i].key.Ref(), r)
	})
}

func MapFromVal(v Value) Map {
	return v.(Map)
}
