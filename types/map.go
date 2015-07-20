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

func mapFromFutures(f []future, cs chunks.ChunkSource) Map {
	return newMapFromData(buildMapData(mapData{}, f), cs)
}

func (fm Map) Len() uint64 {
	return uint64(len(fm.m))
}

func (fm Map) Has(key Value) bool {
	idx := indexMapData(fm.m, key.Ref())
	return idx < len(fm.m) && fm.m[idx].key.Ref() == key.Ref()
}

func (fm Map) Get(key Value) Value {
	idx := indexMapData(fm.m, key.Ref())
	if idx < len(fm.m) {
		entry := fm.m[idx]
		if entry.key.Ref() == key.Ref() {
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
	if idx == len(fm.m) || fm.m[idx].key.Ref() != k.Ref() {
		return fm
	}

	return newMapFromData(append(fm.m[:idx], fm.m[idx+1:]...), fm.cs)
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

type mapEntry struct {
	key   future
	value future
}

func newMapFromData(m mapData, cs chunks.ChunkSource) Map {
	return Map{m, cs, &ref.Ref{}}
}

func buildMapData(oldData mapData, futures []future) mapData {
	// Sadly, Chk.Equals() costs too much.
	Chk.True(0 == len(futures)%2, "Must specify even number of key/value pairs")

	m := make(mapData, len(oldData), len(oldData)+len(futures))
	copy(m, oldData)
	for i := 0; i < len(futures); i += 2 {
		k := futures[i]
		v := futures[i+1]
		e := mapEntry{k, v}
		idx := indexMapData(m, k.Ref())
		if idx != len(m) && m[idx].key.Ref() == k.Ref() {
			m[idx] = e
		} else {
			m = append(m, e)
		}
	}
	sort.Sort(m)
	return m
}

func indexMapData(m mapData, r ref.Ref) int {
	return sort.Search(len(m), func(i int) bool {
		return !ref.Less(m[i].key.Ref(), r)
	})
}

func (md mapData) Len() int {
	return len(md)
}

func (md mapData) Less(i, j int) bool {
	return ref.Less(md[i].key.Ref(), md[j].key.Ref())
}

func (md mapData) Swap(i, j int) {
	md[i], md[j] = md[j], md[i]
}
