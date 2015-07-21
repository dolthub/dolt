package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/ref"
)

type setData []future

type Set struct {
	m   setData // sorted by Ref()
	cs  chunks.ChunkSource
	ref *ref.Ref
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
	idx := indexSetData(fs.m, v.Ref())
	return idx < len(fs.m) && futureEqualsValue(fs.m[idx], v)
}

func (fs Set) Insert(values ...Value) Set {
	return newSetFromData(buildSetData(fs.m, valuesToFutures(values)), fs.cs)
}

func (fs Set) Remove(values ...Value) Set {
	m2 := copySetData(fs.m)
	for _, v := range values {
		if v != nil {
			idx := indexSetData(fs.m, v.Ref())
			if idx < len(fs.m) && futureEqualsValue(fs.m[idx], v) {
				m2 = append(m2[:idx], m2[idx+1:]...)
			}
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
	return ensureRef(fs.ref, fs)
}

func (fs Set) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return fs.Ref() == other.Ref()
	}
}

func newSetFromData(m setData, cs chunks.ChunkSource) Set {
	return Set{m, cs, &ref.Ref{}}
}

func copySetData(m setData) setData {
	r := make(setData, len(m))
	copy(r, m)
	return r
}

func buildSetData(old setData, futures []future) setData {
	r := make(setData, len(old), len(old)+len(futures))
	copy(r, old)
	for _, f := range futures {
		idx := indexSetData(r, f.Ref())
		if idx < len(r) && futuresEqual(r[idx], f) {
			// We already have this fellow.
			continue
		} else {
			r = append(r, f)
		}
	}
	sort.Sort(r)
	return r
}

func indexSetData(m setData, r ref.Ref) int {
	return sort.Search(len(m), func(i int) bool {
		return !ref.Less(m[i].Ref(), r)
	})
}

func (sd setData) Len() int {
	return len(sd)
}

func (sd setData) Less(i, j int) bool {
	return ref.Less(sd[i].Ref(), sd[j].Ref())
}

func (sd setData) Swap(i, j int) {
	sd[i], sd[j] = sd[j], sd[i]
}
