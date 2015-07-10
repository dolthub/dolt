package types

import (
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

type mapData map[ref.Ref]MapEntry

type Map struct {
	m   mapData
	ref *ref.Ref
}

func NewMap(kv ...Value) Map {
	return newMapFromData(buildMapData(mapData{}, kv...))
}

func (fm Map) Len() uint64 {
	return uint64(len(fm.m))
}

func (fm Map) Has(key Value) bool {
	_, ok := fm.m[key.Ref()]
	return ok
}

func (fm Map) Get(key Value) Value {
	if v, ok := fm.m[key.Ref()]; ok {
		return v.Value
	} else {
		return nil
	}
}

func (fm Map) Set(key Value, val Value) Map {
	return newMapFromData(buildMapData(fm.m, key, val))
}

func (fm Map) SetM(kv ...Value) Map {
	return newMapFromData(buildMapData(fm.m, kv...))
}

func (fm Map) Remove(k Value) Map {
	m := copyMapData(fm.m)
	delete(m, k.Ref())
	return newMapFromData(m)
}

type mapIterCallback func(entry MapEntry) bool

func (fm Map) Iter(cb mapIterCallback) {
	for _, v := range fm.m {
		if cb(v) {
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

type MapEntry struct {
	Key   Value
	Value Value
}

type MapEntrySlice []MapEntry

func (mes MapEntrySlice) Len() int {
	return len(mes)
}

func (mes MapEntrySlice) Swap(i, j int) {
	mes[i], mes[j] = mes[j], mes[i]
}

func (mes MapEntrySlice) Less(i, j int) bool {
	return ref.Less(mes[i].Key.Ref(), mes[j].Key.Ref())
}

func newMapFromData(m mapData) Map {
	return Map{m, &ref.Ref{}}
}

func copyMapData(m mapData) mapData {
	r := mapData{}
	for k, v := range m {
		r[k] = v
	}
	return r
}

func buildMapData(oldData mapData, kv ...Value) mapData {
	Chk.Equal(0, len(kv)%2, "Must specify even number of key/value pairs")

	m := copyMapData(oldData)
	for i := 0; i < len(kv); i += 2 {
		k := kv[i]
		v := kv[i+1]
		m[k.Ref()] = MapEntry{k, v}
	}
	return m
}
