package types

import (
	"sort"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type Map struct {
	data mapData // sorted by entry.key.Ref()
	ref  *ref.Ref
}

type mapData []mapEntry

func NewMap(kv ...Value) Map {
	return newMapFromData(buildMapData(mapData{}, kv))
}

func (fm Map) First() (Value, Value) {
	if len(fm.data) == 0 {
		return nil, nil
	} else {
		entry := fm.data[0]
		return entry.key, entry.value
	}
}

func (fm Map) Len() uint64 {
	return uint64(len(fm.data))
}

func (fm Map) Empty() bool {
	return fm.Len() == uint64(0)
}

func (fm Map) Has(key Value) bool {
	idx := indexMapData(fm.data, key.Ref())
	return idx < len(fm.data) && fm.data[idx].key.Equals(key)
}

func (fm Map) Get(key Value) Value {
	if v, ok := fm.MaybeGet(key); ok {
		return v
	}
	return nil
}

func (fm Map) MaybeGet(key Value) (v Value, ok bool) {
	idx := indexMapData(fm.data, key.Ref())
	if idx < len(fm.data) {
		entry := fm.data[idx]
		if entry.key.Equals(key) {
			return entry.value, true
		}
	}
	return
}

func (fm Map) Set(key Value, val Value) Map {
	return newMapFromData(buildMapData(fm.data, []Value{key, val}))
}

func (fm Map) SetM(kv ...Value) Map {
	return newMapFromData(buildMapData(fm.data, kv))
}

func (fm Map) Remove(k Value) Map {
	idx := indexMapData(fm.data, k.Ref())
	if idx == len(fm.data) || !fm.data[idx].key.Equals(k) {
		return fm
	}

	m := make(mapData, len(fm.data)-1)
	copy(m, fm.data[:idx])
	copy(m[idx:], fm.data[idx+1:])
	return newMapFromData(m)
}

type mapIterCallback func(key, value Value) (stop bool)

func (fm Map) Iter(cb mapIterCallback) {
	for _, entry := range fm.data {
		if cb(entry.key, entry.value) {
			break
		}
	}
}

type mapIterAllCallback func(key, value Value)

func (fm Map) IterAll(cb mapIterAllCallback) {
	for _, entry := range fm.data {
		cb(entry.key, entry.value)
	}
}

type mapFilterCallback func(key, value Value) (keep bool)

func (fm Map) Filter(cb mapFilterCallback) Map {
	data := mapData{}
	for _, entry := range fm.data {
		if cb(entry.key, entry.value) {
			data = append(data, entry)
		}
	}
	// Already sorted.
	return newMapFromData(data)
}

func (fm Map) Ref() ref.Ref {
	return EnsureRef(fm.ref, fm)
}

func (m Map) Equals(other Value) (res bool) {
	if other, ok := other.(Map); ok {
		return m.Ref() == other.Ref()
	}
	return false
}

func (fm Map) Chunks() (futures []Future) {
	for _, entry := range fm.data {
		futures = appendValueToChunks(futures, entry.key)
		futures = appendValueToChunks(futures, entry.value)
	}
	return
}

var mapTypeRef = MakeCompoundTypeRef(MapKind, MakePrimitiveTypeRef(ValueKind), MakePrimitiveTypeRef(ValueKind))

func (fm Map) TypeRef() TypeRef {
	return mapTypeRef
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

func newMapFromData(data mapData) Map {
	return Map{data, &ref.Ref{}}
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
