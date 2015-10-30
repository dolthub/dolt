package types

import (
	"sort"

	"github.com/attic-labs/noms/ref"
)

type setData []Value

type Set struct {
	data setData // sorted by Ref()
	t    TypeRef
	ref  *ref.Ref
}

func NewSet(v ...Value) Set {
	return newSetFromData(buildSetData(setData{}, v), setTypeRef)
}

func (s Set) Empty() bool {
	return s.Len() == uint64(0)
}

func (s Set) Len() uint64 {
	return uint64(len(s.data))
}

func (s Set) Has(v Value) bool {
	idx := indexSetData(s.data, v.Ref())
	return idx < len(s.data) && s.data[idx].Equals(v)
}

func (s Set) Insert(values ...Value) Set {
	return newSetFromData(buildSetData(s.data, values), s.t)
}

func (s Set) Remove(values ...Value) Set {
	data := copySetData(s.data)
	for _, v := range values {
		if v != nil {
			idx := indexSetData(s.data, v.Ref())
			if idx < len(s.data) && s.data[idx].Equals(v) {
				data = append(data[:idx], data[idx+1:]...)
			}
		}
	}
	return newSetFromData(data, s.t)
}

func (s Set) Union(others ...Set) Set {
	result := s
	for _, other := range others {
		other.Iter(func(v Value) (stop bool) {
			result = result.Insert(v)
			return
		})
	}
	return result
}

func (s Set) Subtract(others ...Set) Set {
	result := s
	for _, other := range others {
		other.Iter(func(v Value) (stop bool) {
			result = result.Remove(v)
			return
		})
	}
	return result
}

type setIterCallback func(v Value) bool

func (s Set) Iter(cb setIterCallback) {
	for _, v := range s.data {
		if cb(v) {
			break
		}
	}
}

type setIterAllCallback func(v Value)

func (s Set) IterAll(cb setIterAllCallback) {
	for _, v := range s.data {
		cb(v)
	}
}

type setFilterCallback func(v Value) (keep bool)

func (s Set) Filter(cb setFilterCallback) Set {
	data := setData{}
	for _, v := range s.data {
		if cb(v) {
			data = append(data, v)
		}
	}
	// Already sorted.
	return newSetFromData(data, s.t)
}

func (s Set) Any() Value {
	for _, v := range s.data {
		return v
	}
	return nil
}

func (s Set) Ref() ref.Ref {
	return EnsureRef(s.ref, s)
}

func (s Set) Equals(other Value) bool {
	if other, ok := other.(Set); ok {
		return s.Ref() == other.Ref()
	}
	return false
}

func (s Set) Chunks() (chunks []ref.Ref) {
	for _, v := range s.data {
		chunks = appendValueToChunks(chunks, v)
	}
	return
}

var setTypeRef = MakeCompoundTypeRef(SetKind, MakePrimitiveTypeRef(ValueKind))

func (s Set) TypeRef() TypeRef {
	return s.t
}

func init() {
	RegisterFromValFunction(setTypeRef, func(v Value) Value {
		return v.(Set)
	})
}

func newSetFromData(m setData, t TypeRef) Set {
	return Set{m, t, &ref.Ref{}}
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
