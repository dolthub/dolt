package types

import (
	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

type setData map[ref.Ref]future

type Set struct {
	m  setData
	cs chunks.ChunkSource
	cr *cachedRef
}

func NewSet(v ...Value) Set {
	return newSetFromData(buildSetData(setData{}, valuesToFutures(v)), nil)
}

func setFromFutures(f []future, cs chunks.ChunkSource) Set {
	return newSetFromData(buildSetData(setData{}, f), cs)
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
	return newSetFromData(buildSetData(fs.m, valuesToFutures(values)), fs.cs)
}

func (fs Set) Remove(values ...Value) Set {
	m2 := copySetData(fs.m)
	for _, v := range values {
		if v != nil {
			delete(m2, v.Ref())
		}
	}
	return newSetFromData(m2, fs.cs)
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
	for _, f := range fm.m {
		v, err := f.Deref(fm.cs)
		Chk.NoError(err)
		if cb(v) {
			break
		}
	}
}

func (fm Set) Any() Value {
	for _, f := range fm.m {
		v, err := f.Deref(fm.cs)
		Chk.NoError(err)
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

func newSetFromData(m setData, cs chunks.ChunkSource) Set {
	return Set{m, cs, &cachedRef{}}
}

func copySetData(m setData) setData {
	r := setData{}
	for k, f := range m {
		r[k] = f
	}
	return r
}

func buildSetData(old setData, futures []future) setData {
	m := copySetData(old)
	for _, f := range futures {
		m[f.Ref()] = f
	}
	return m
}
