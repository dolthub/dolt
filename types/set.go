package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	// The window size to use for computing the rolling hash.
	setWindowSize = 1
	setPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type Set struct {
	seq orderedSequence
	ref *ref.Ref
}

var setType = MakeSetType(ValueType)

func newSet(seq orderedSequence) Set {
	return Set{seq, &ref.Ref{}}
}

func NewSet(v ...Value) Set {
	return NewTypedSet(setType, v...)
}

func NewTypedSet(t *Type, v ...Value) Set {
	d.Chk.Equal(SetKind, t.Kind(), "Invalid type. Expected:SetKind, found: %s", t.Describe())
	return newTypedSet(t, buildSetData([]Value{}, v, t)...)
}

func newTypedSet(t *Type, data ...Value) Set {
	seq := newEmptySequenceChunker(makeSetLeafChunkFn(t, nil), newOrderedMetaSequenceChunkFn(t, nil), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	for _, v := range data {
		seq.Append(v)
	}

	return seq.Done().(Set)
}

func (s Set) Type() *Type {
	return s.seq.Type()
}

func (s Set) Equals(other Value) bool {
	return other != nil && s.Ref() == other.Ref()
}

func (s Set) Less(other Value) bool {
	return valueLess(s, other)
}

func (s Set) Ref() ref.Ref {
	return EnsureRef(s.ref, s)
}

func (s Set) Len() uint64 {
	return s.seq.numLeaves()
}

func (s Set) Empty() bool {
	return s.Len() == 0
}

func (s Set) Chunks() []Ref {
	return s.seq.Chunks()
}

func (s Set) sequence() sequence {
	return s.seq
}

func (s Set) ChildValues() (values []Value) {
	s.IterAll(func(v Value) {
		values = append(values, v)
	})
	return
}

func (s Set) First() Value {
	cur := newCursorAtKey(s.seq, nil, false, false)
	if !cur.valid() {
		return nil
	}
	return cur.current().(Value)
}

func (s Set) Insert(values ...Value) Set {
	if len(values) == 0 {
		return s
	}

	assertType(s.elemType(), values...)

	head, tail := values[0], values[1:]

	var res Set
	if cur, found := s.getCursorAtValue(head); !found {
		res = s.splice(cur, 0, head)
	} else {
		res = s
	}

	return res.Insert(tail...)
}

func (s Set) Remove(values ...Value) Set {
	if len(values) == 0 {
		return s
	}

	head, tail := values[0], values[1:]

	var res Set
	if cur, found := s.getCursorAtValue(head); found {
		res = s.splice(cur, 1)
	} else {
		res = s
	}

	return res.Remove(tail...)
}

func (s Set) splice(cur *sequenceCursor, deleteCount uint64, vs ...Value) Set {
	assertType(s.elemType(), vs...)
	ch := newSequenceChunker(cur, makeSetLeafChunkFn(s.seq.Type(), s.seq.valueReader()), newOrderedMetaSequenceChunkFn(s.seq.Type(), s.seq.valueReader()), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)
	for deleteCount > 0 {
		ch.Skip()
		deleteCount--
	}

	for _, v := range vs {
		ch.Append(v)
	}
	return ch.Done().(Set)
}

func (s Set) getCursorAtValue(v Value) (cur *sequenceCursor, found bool) {
	cur = newCursorAtKey(s.seq, v, true, false)
	found = cur.idx < cur.seq.seqLen() && cur.current().(Value).Equals(v)
	return
}

func (s Set) Has(key Value) bool {
	cur := newCursorAtKey(s.seq, key, false, false)
	return cur.valid() && cur.current().(Value).Equals(key)
}

type setIterCallback func(v Value) bool

func (s Set) Iter(cb setIterCallback) {
	cur := newCursorAtKey(s.seq, nil, false, false)
	cur.iter(func(v interface{}) bool {
		return cb(v.(Value))
	})
}

type setIterAllCallback func(v Value)

func (s Set) IterAll(cb setIterAllCallback) {
	cur := newCursorAtKey(s.seq, nil, false, false)
	cur.iter(func(v interface{}) bool {
		cb(v.(Value))
		return false
	})
}

func (s Set) elemType() *Type {
	return s.Type().Desc.(CompoundDesc).ElemTypes[0]
}

func buildSetData(old []Value, values []Value, t *Type) []Value {
	elemType := t.Desc.(CompoundDesc).ElemTypes[0]

	data := make([]Value, len(old), len(old)+len(values))
	copy(data, old)
	for _, v := range values {
		assertType(elemType, v)
		idx := indexOfSetValue(data, v)
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

func indexOfSetValue(m []Value, v Value) int {
	return sort.Search(len(m), func(i int) bool {
		return !m[i].Less(v)
	})
}

func newSetLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(setWindowSize, sha1.Size, setPattern, func(item sequenceItem) []byte {
		digest := item.(Value).Ref().Digest()
		return digest[:]
	})
}

func makeSetLeafChunkFn(t *Type, vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		setData := make([]Value, len(items), len(items))

		for i, v := range items {
			setData[i] = v.(Value)
		}

		set := newSet(newSetLeafSequence(t, vr, setData...))

		var indexValue Value
		if len(setData) > 0 {
			indexValue = setData[len(setData)-1]
			if !isKindOrderedByValue(indexValue.Type().Kind()) {
				indexValue = NewRef(indexValue)
			}
		}

		return newMetaTuple(indexValue, set, NewRef(set), uint64(len(items))), set
	}
}
