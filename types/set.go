package types

import (
	"sort"

	"github.com/attic-labs/noms/ref"
)

type setData []Value

type Set struct {
	data setData // sorted by Ref()
	ref  *ref.Ref
}

func NewSet(v ...Value) Set {
	return newSetFromData(buildSetData(setData{}, v))
}

func (fs Set) Empty() bool {
	return fs.Len() == uint64(0)
}

func (fs Set) Len() uint64 {
	return uint64(len(fs.data))
}

func (fs Set) Has(v Value) bool {
	idx := indexSetData(fs.data, v.Ref())
	return idx < len(fs.data) && fs.data[idx].Equals(v)
}

func (fs Set) Insert(values ...Value) Set {
	return newSetFromData(buildSetData(fs.data, values))
}

func (fs Set) Remove(values ...Value) Set {
	data := copySetData(fs.data)
	for _, v := range values {
		if v != nil {
			idx := indexSetData(fs.data, v.Ref())
			if idx < len(fs.data) && fs.data[idx].Equals(v) {
				data = append(data[:idx], data[idx+1:]...)
			}
		}
	}
	return newSetFromData(data)
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
	for _, v := range fm.data {
		if cb(v) {
			break
		}
	}
}

type setIterAllCallback func(v Value)

func (fm Set) IterAll(cb setIterAllCallback) {
	for _, v := range fm.data {
		cb(v)
	}
}

type setFilterCallback func(v Value) (keep bool)

func (fm Set) Filter(cb setFilterCallback) Set {
	data := setData{}
	for _, v := range fm.data {
		if cb(v) {
			data = append(data, v)
		}
	}
	// Already sorted.
	return newSetFromData(data)
}

func (fm Set) Any() Value {
	for _, v := range fm.data {
		return v
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
	for _, v := range fs.data {
		futures = appendValueToChunks(futures, v)
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

func newSetFromData(m setData) Set {
	return Set{m, &ref.Ref{}}
}

func copySetData(m setData) setData {
	r := make(setData, len(m))
	copy(r, m)
	return r
}

func buildSetData(old setData, values []Value) setData {
	data := make(setData, len(old), len(old)+len(values))
	copy(data, old)
	for _, v := range values {
		idx := indexSetData(data, v.Ref())
		if idx < len(data) && data[idx].Equals(v) {
			// We already have this fellow.
			continue
		}
		// TODO: These repeated copies suck. We're not allocating more memory (because we made the slice with the correct capacity to begin with above - yay!), but still, this is more work than necessary. Perhaps we should use an actual BST for the in-memory state, rather than a flat list.
		data = append(data, nil)
		copy(data[idx+1:], data[idx:])
		data[idx] = v
	}
	return data
}

func indexSetData(m setData, r ref.Ref) int {
	return sort.Search(len(m), func(i int) bool {
		return !ref.Less(m[i].Ref(), r)
	})
}

func SetFromVal(v Value) Set {
	return v.(Set)
}
