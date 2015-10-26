package types

import (
	"sort"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type setData []Future

type Set struct {
	m   setData // sorted by Ref()
	cs  chunks.ChunkSource
	ref *ref.Ref
}

func NewSet(v ...Value) Set {
	return newSetFromData(buildSetData(setData{}, valuesToFutures(v)), nil)
}

func setFromFutures(f []Future, cs chunks.ChunkSource) Set {
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
		v := f.Deref(fm.cs)
		f.Release()
		if cb(v) {
			break
		}
	}
}

type setIterAllCallback func(v Value)

func (fm Set) IterAll(cb setIterAllCallback) {
	// TODO: sort iteration order
	for _, f := range fm.m {
		cb(f.Deref(fm.cs))
		f.Release()
	}
}

type setFilterCallback func(v Value) (keep bool)

func (fm Set) Filter(cb setFilterCallback) Set {
	ns := NewSet()
	// TODO: sort iteration order
	for _, f := range fm.m {
		v := f.Deref(fm.cs)
		if cb(v) {
			ns = ns.Insert(v)
		}
		f.Release()
	}
	return ns
}

func (fm Set) Any() Value {
	for _, f := range fm.m {
		return f.Deref(fm.cs)
	}
	return nil
}

func (fs Set) Ref() ref.Ref {
	return EnsureRef(fs.ref, fs)
}

func (fs Set) Equals(other Value) bool {
	if other, ok := other.(Set); ok {
		return fs.Ref() == other.Ref()
	}
	return false
}

func (fs Set) Chunks() (futures []Future) {
	for _, f := range fs.m {
		futures = appendChunks(futures, f)
	}
	return
}

var setTypeRef = MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(ValueKind))

func (fs Set) TypeRef() TypeRef {
	return setTypeRef
}

func init() {
	RegisterFromValFunction(setTypeRef, func(v Value) Value {
		return v.(Set)
	})
}

func newSetFromData(m setData, cs chunks.ChunkSource) Set {
	return Set{m, cs, &ref.Ref{}}
}

func copySetData(m setData) setData {
	r := make(setData, len(m))
	copy(r, m)
	return r
}

func buildSetData(old setData, futures []Future) setData {
	r := make(setData, len(old), len(old)+len(futures))
	copy(r, old)
	for _, f := range futures {
		idx := indexSetData(r, f.Ref())
		if idx < len(r) && futuresEqual(r[idx], f) {
			// We already have this fellow.
			continue
		}
		// TODO: These repeated copies suck. We're not allocating more memory (because we made the slice with the correct capacity to begin with above - yay!), but still, this is more work than necessary. Perhaps we should use an actual BST for the in-memory state, rather than a flat list.
		r = append(r, nil)
		copy(r[idx+1:], r[idx:])
		r[idx] = f
	}
	return r
}

func indexSetData(m setData, r ref.Ref) int {
	return sort.Search(len(m), func(i int) bool {
		return !ref.Less(m[i].Ref(), r)
	})
}

func SetFromVal(v Value) Set {
	return v.(Set)
}
