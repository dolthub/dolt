package types

import (
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

type mapData map[ref.Ref]MapEntry

type flatMap struct {
	m  mapData
	cr *cachedRef
}

func newFlatMap(m mapData) flatMap {
	return flatMap{m, &cachedRef{}}
}

func (fm flatMap) Len() uint64 {
	return uint64(len(fm.m))
}

func (fm flatMap) Has(key Value) bool {
	_, ok := fm.m[key.Ref()]
	return ok
}

func (fm flatMap) Get(key Value) Value {
	if v, ok := fm.m[key.Ref()]; ok {
		return v.Value
	} else {
		return nil
	}
}

func (fm flatMap) Set(key Value, val Value) Map {
	return newFlatMap(buildMapData(fm.m, key, val))
}

func (fm flatMap) SetM(kv ...Value) Map {
	return newFlatMap(buildMapData(fm.m, kv...))
}

func (fm flatMap) Remove(k Value) Map {
	m := copyMapData(fm.m)
	delete(m, k.Ref())
	return newFlatMap(m)
}

func (fm flatMap) Iter(cb mapIterCallback) {
	for _, v := range fm.m {
		if cb(v) {
			break
		}
	}
}

func (fm flatMap) Ref() ref.Ref {
	return fm.cr.Ref(fm)
}

func (fm flatMap) Equals(other Value) (res bool) {
	if other == nil {
		return false
	} else {
		return fm.Ref() == other.Ref()
	}
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
