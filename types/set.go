package types

import (
	"github.com/attic-labs/noms/ref"
)

type setData map[ref.Ref]Value

type Set struct {
	m  setData
	cr *cachedRef
}

func NewSet(v ...Value) Set {
	return newSetFromData(buildSetData(setData{}, v))
}

func (fs Set) Empty() bool {
	return fs.Len() == uint64(0)
}

func (fs Set) Len() uint64 {
	return uint64(len(fs.m))
}

func (fs Set) Has(v Value) bool {
	_, ok := fs.m[v.Ref()]
	return ok
}

func (fs Set) Insert(values ...Value) Set {
	return newSetFromData(buildSetData(fs.m, values))
}

func (fs Set) Remove(values ...Value) Set {
	m2 := copySetData(fs.m)
	for _, v := range values {
		if v != nil {
			delete(m2, v.Ref())
		}
	}
	return newSetFromData(m2)
}

func (fs Set) Union(others ...Set) (result Set) {
	result = fs
	for _, other := range others {
		other.Iter(func(v Value) (stop bool) {
			result = result.Insert(v)
			return
		})
	}
	return result
}

func (fs Set) Subtract(others ...Set) (result Set) {
	result = fs
	for _, other := range others {
		other.Iter(func(v Value) (stop bool) {
			result = result.Remove(v)
			return
		})
	}
	return result
}

type setIterCallback func(v Value) bool

func (fm Set) Iter(cb setIterCallback) {
	// TODO: sort iteration order
	for _, v := range fm.m {
		if cb(v) {
			break
		}
	}
}

func (fm Set) Any() Value {
	for _, v := range fm.m {
		return v
	}
	return nil
}

func (fs Set) Ref() ref.Ref {
	return fs.cr.Ref(fs)
}

func (fs Set) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return fs.Ref() == other.Ref()
	}
}

func newSetFromData(m setData) Set {
	return Set{
		m:  m,
		cr: &cachedRef{},
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
