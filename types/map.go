package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
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
	idx := indexMapData(fm.m, key.Ref())
	if idx < len(fm.m) {
		entry := fm.m[idx]
		if futureEqualsValue(entry.key, key) {
			v, err := entry.value.Deref(fm.cs)
			Chk.NoError(err)
			return v
		}
	}
	return nil
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

type mapIterCallback func(key, value Value) bool

func (fm Map) Iter(cb mapIterCallback) {
	for _, entry := range fm.m {
		k, err := entry.key.Deref(fm.cs)
		Chk.NoError(err)
		v, err := entry.value.Deref(fm.cs)
		Chk.NoError(err)
		if cb(k, v) {
			break
		}
	}
}

func (fm Map) Ref() ref.Ref {
	return ensureRef(fm.ref, fm)
}

func (fm Map) Equals(other Value) (res bool) {
	if other == nil {
		return false
	} else {
		return fm.Ref() == other.Ref()
	}
}

func (fm Map) Futures() (futures []Future) {
	appendIfUnresolved := func(f Future) {
		switch f.(type) {
		case *unresolvedFuture:
			futures = append(futures, f)
		default:
		}
	}
	for _, entry := range fm.m {
		appendIfUnresolved(entry.key)
		appendIfUnresolved(entry.value)
	}
	return
}

type mapEntry struct {
	key   Future
	value Future
}

func newMapFromData(m mapData, cs chunks.ChunkSource) Map {
	return Map{m, cs, &ref.Ref{}}
}

func buildMapData(oldData mapData, futures []Future) mapData {
	// Sadly, Chk.Equals() costs too much. BUG #83
	Chk.True(0 == len(futures)%2, "Must specify even number of key/value pairs")

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
