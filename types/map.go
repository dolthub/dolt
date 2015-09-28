package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type Map struct {
	m   mapData // sorted by entry.key.Ref()
	cs  chunks.ChunkSource
	ref *ref.Ref
}

type mapData []mapEntry

func NewMap(kv ...Value) Map {
	return newMapFromData(buildMapData(mapData{}, valuesToFutures(kv)), nil)
}

func mapFromFutures(f []Future, cs chunks.ChunkSource) Map {
	return newMapFromData(buildMapData(mapData{}, f), cs)
}

func (fm Map) First() (Value, Value) {
	if len(fm.m) == 0 {
		return nil, nil
	} else {
		entry := fm.m[0]
		return entry.key.Deref(fm.cs), entry.value.Deref(fm.cs)
	}
}

func (fm Map) Len() uint64 {
	return uint64(len(fm.m))
}

func (fm Map) Empty() bool {
	return fm.Len() == uint64(0)
}

func (fm Map) Has(key Value) bool {
	idx := indexMapData(fm.m, key.Ref())
	return idx < len(fm.m) && futureEqualsValue(fm.m[idx].key, key)
}

func (fm Map) Get(key Value) Value {
	if v, ok := fm.MaybeGet(key); ok {
		return v
	}
	return nil
}

func (fm Map) MaybeGet(key Value) (v Value, ok bool) {
	idx := indexMapData(fm.m, key.Ref())
	if idx < len(fm.m) {
		entry := fm.m[idx]
		if futureEqualsValue(entry.key, key) {
			return entry.value.Deref(fm.cs), true
		}
	}
	return
}

func (fm Map) Set(key Value, val Value) Map {
	return newMapFromData(buildMapData(fm.m, valuesToFutures([]Value{key, val})), fm.cs)
}

func (fm Map) SetM(kv ...Value) Map {
	return newMapFromData(buildMapData(fm.m, valuesToFutures(kv)), fm.cs)
}

func (fm Map) Remove(k Value) Map {
	idx := indexMapData(fm.m, k.Ref())
	if idx == len(fm.m) || !futureEqualsValue(fm.m[idx].key, k) {
		return fm
	}

	m := make(mapData, len(fm.m)-1)
	copy(m, fm.m[:idx])
	copy(m[idx:], fm.m[idx+1:])
	return newMapFromData(m, fm.cs)
}

type mapIterCallback func(key, value Value) (stop bool)

func (fm Map) Iter(cb mapIterCallback) {
	for _, entry := range fm.m {
		k := entry.key.Deref(fm.cs)
		v := entry.value.Deref(fm.cs)
		entry.key.Release()
		entry.value.Release()
		if cb(k, v) {
			break
		}
	}
}

type mapIterAllCallback func(key, value Value)

func (fm Map) IterAll(cb mapIterAllCallback) {
	for _, entry := range fm.m {
		k := entry.key.Deref(fm.cs)
		v := entry.value.Deref(fm.cs)
		cb(k, v)
		entry.key.Release()
		entry.value.Release()
	}
}

type mapFilterCallback func(key, value Value) (keep bool)

func (fm Map) Filter(cb mapFilterCallback) Map {
	nm := NewMap()
	for _, entry := range fm.m {
		k := entry.key.Deref(fm.cs)
		v := entry.value.Deref(fm.cs)
		if cb(k, v) {
			nm = nm.Set(k, v)
		}
		entry.key.Release()
		entry.value.Release()
	}
	return nm
}

func (fm Map) Ref() ref.Ref {
	return ensureRef(fm.ref, fm)
}

func (m Map) Equals(other Value) (res bool) {
	if other, ok := other.(Map); ok {
		return m.Ref() == other.Ref()
	}
	return false
}

func (fm Map) Chunks() (futures []Future) {
	appendIfUnresolved := func(f Future) {
		if f, ok := f.(*unresolvedFuture); ok {
			futures = append(futures, f)
		}
	}
	for _, entry := range fm.m {
		appendIfUnresolved(entry.key)
		appendIfUnresolved(entry.value)
	}
	return
}

func (fm Map) TypeRef() TypeRef {
	if v, ok := fm.MaybeGet(NewString("$type")); ok {
		return v.(TypeRef)
	}
	// TODO: The key and value type needs to be configurable.
	return MakeCompoundTypeRef("", MapKind, MakePrimitiveTypeRef(ValueKind), MakePrimitiveTypeRef(ValueKind))
}

type mapEntry struct {
	key   Future
	value Future
}

func newMapFromData(m mapData, cs chunks.ChunkSource) Map {
	return Map{m, cs, &ref.Ref{}}
}

func buildMapData(oldData mapData, futures []Future) mapData {
	// Sadly, d.Chk.Equals() costs too much. BUG #83
	d.Chk.True(0 == len(futures)%2, "Must specify even number of key/value pairs")

	m := make(mapData, len(oldData), len(oldData)+len(futures))
	copy(m, oldData)
	for i := 0; i < len(futures); i += 2 {
		k := futures[i]
		v := futures[i+1]
		idx := indexMapData(m, k.Ref())
		if idx < len(m) && futuresEqual(m[idx].key, k) {
			if !futuresEqual(m[idx].value, v) {
				m[idx] = mapEntry{k, v}
			}
			continue
		}

		// TODO: These repeated copies suck. We're not allocating more memory (because we made the slice with the correct capacity to begin with above - yay!), but still, this is more work than necessary. Perhaps we should use an actual BST for the in-memory state, rather than a flat list.
		m = append(m, mapEntry{})
		copy(m[idx+1:], m[idx:])
		m[idx] = mapEntry{k, v}
	}
	return m
}

func indexMapData(m mapData, r ref.Ref) int {
	return sort.Search(len(m), func(i int) bool {
		return !ref.Less(m[i].key.Ref(), r)
	})
}

func MapFromVal(v Value) Map {
	return v.(Map)
}
