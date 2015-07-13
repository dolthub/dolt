package types

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

type mapData map[ref.Ref]mapEntry

type Map struct {
	m   mapData
	cs  chunks.ChunkSource
	ref *ref.Ref
}

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
	_, ok := fm.m[key.Ref()]
	return ok
}

func (fm Map) Get(key Value) Value {
	if entry, ok := fm.m[key.Ref()]; ok {
		v, err := entry.value.Deref(fm.cs)
		Chk.NoError(err)
		return v
	} else {
		return nil
	}
}

func (fm Map) Set(key Value, val Value) Map {
	return newMapFromData(buildMapData(fm.m, valuesToFutures([]Value{key, val})), nil)
}

func (fm Map) SetM(kv ...Value) Map {
	return newMapFromData(buildMapData(fm.m, valuesToFutures(kv)), nil)
}

func (fm Map) Remove(k Value) Map {
	m := copyMapData(fm.m)
	delete(m, k.Ref())
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

type mapEntry struct {
	key   future
	value future
}

func newMapFromData(m mapData, cs chunks.ChunkSource) Map {
	return Map{m, cs, &ref.Ref{}}
}

func copyMapData(m mapData) mapData {
	r := mapData{}
	for k, v := range m {
		r[k] = v
	}
	return r
}

func buildMapData(oldData mapData, futures []future) mapData {
	Chk.Equal(0, len(futures)%2, "Must specify even number of key/value pairs")

	m := copyMapData(oldData)
	for i := 0; i < len(futures); i += 2 {
		k := futures[i]
		v := futures[i+1]
		m[k.Ref()] = mapEntry{k, v}
	}
	return m
}
