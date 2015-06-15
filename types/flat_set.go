package types

import (
	"github.com/attic-labs/noms/ref"
)

type setData map[ref.Ref]Value

type flatSet struct {
	m  setData
	cr *cachedRef
}

func newFlatSet(m setData) flatSet {
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
	return newFlatSet(buildSetData(fs.m, values))
}

func (fs flatSet) Remove(values ...Value) Set {
	m2 := copySetData(fs.m)
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

func copySetData(m setData) setData {
	r := setData{}
	for k, v := range m {
		r[k] = v
	}
	return r
}

func buildSetData(old setData, values []Value) setData {
	m := copySetData(old)
	for _, v := range values {
		m[v.Ref()] = v
	}
	return m
}
