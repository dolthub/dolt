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

func (fs flatSet) Empty() bool {
	return fs.Len() == uint64(0)
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
		if v != nil {
			delete(m2, v.Ref())
		}
	}
	return newFlatSet(m2)
}

func (fs flatSet) Union(others ...Set) (result Set) {
	result = fs
	for _, other := range others {
		other.Iter(func(v Value) (stop bool) {
			result = result.Insert(v)
			return
		})
	}
	return result
}

func (fs flatSet) Subtract(others ...Set) (result Set) {
	result = fs
	for _, other := range others {
		other.Iter(func(v Value) (stop bool) {
			result = result.Remove(v)
			return
		})
	}
	return result
}

func (fm flatSet) Iter(cb setIterCallback) {
	// TODO: sort iteration order
	for _, v := range fm.m {
		if cb(v) {
			break
		}
	}
}

func (fm flatSet) Any() Value {
	for _, v := range fm.m {
		return v
	}
	return nil
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
