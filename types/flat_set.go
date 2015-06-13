package types

import (
	"github.com/attic-labs/noms/ref"
)

type flatSet struct {
	m  setInternalMap
	cr *cachedRef
}

type setInternalMap map[ref.Ref]Value

func newFlatSet(m setInternalMap) flatSet {
	return flatSet{
		m:  m,
		cr: &cachedRef{},
	}
}

func (fs flatSet) Len() uint64 {
	return uint64(len(fs.m))
}

func (fs flatSet) Has(v Value) bool {
	_, ok := fs.m[v.Ref()]
	return ok
}

func (fs flatSet) Insert(values ...Value) Set {
	return newFlatSet(buildInternalMap(fs.m, values))
}

func (fs flatSet) Remove(values ...Value) Set {
	m2 := copyInternalMap(fs.m)
	for _, v := range values {
		delete(m2, v.Ref())
	}
	return newFlatSet(m2)
}

func (fm flatSet) Iter(cb setIterCallback) {
	// TODO: sort iteration order
	for _, v := range fm.m {
		if cb(v) {
			break
		}
	}
}

func (fs flatSet) Ref() ref.Ref {
	return fs.cr.Ref(fs)
}

func (fs flatSet) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return fs.Ref() == other.Ref()
	}
}

func copyInternalMap(m setInternalMap) setInternalMap {
	r := setInternalMap{}
	for k, v := range m {
		r[k] = v
	}
	return r
}

func buildInternalMap(old setInternalMap, values []Value) setInternalMap {
	m := copyInternalMap(old)
	for _, v := range values {
		m[v.Ref()] = v
	}
	return m
}
