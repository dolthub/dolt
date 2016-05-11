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
	if ch, found := setSequenceChunkerAtValue(s.seq, head); !found {
		ch.Append(head)
		res = ch.Done().(Set)
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
	if ch, found := setSequenceChunkerAtValue(s.seq, head); found {
		ch.Skip()
		res = ch.Done().(Set)
	} else {
		res = s
	}

	return res.Remove(tail...)
}

func setSequenceChunkerAtValue(seq orderedSequence, v Value) (*sequenceChunker, bool) {
	cur := newCursorAtKey(seq, v, true, false)
	found := cur.idx < cur.seq.seqLen() && cur.current().(Value).Equals(v)
	ch := newSequenceChunker(cur, makeSetLeafChunkFn(seq.Type(), seq.valueReader()), newOrderedMetaSequenceChunkFn(seq.Type(), seq.valueReader()), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)
	return ch, found
}

type setFilterCallback func(v Value) (keep bool)

func (s Set) Filter(cb setFilterCallback) Set {
	seq := s.seq
	ch := newEmptySequenceChunker(makeSetLeafChunkFn(seq.Type(), seq.valueReader()), newOrderedMetaSequenceChunkFn(seq.Type(), seq.valueReader()), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	s.IterAll(func(v Value) {
		if cb(v) {
			ch.Append(v)
		}
	})

	return ch.Done().(Set)
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
	idxFn := getIndexFnForSetType(t)
	elemType := t.Desc.(CompoundDesc).ElemTypes[0]

	data := make([]Value, len(old), len(old)+len(values))
	copy(data, old)
	for _, v := range values {
		assertType(elemType, v)
		idx := idxFn(data, v)
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

type indexOfSetFn func(m []Value, v Value) int

func getIndexFnForSetType(t *Type) indexOfSetFn {
	orderByValue := t.Desc.(CompoundDesc).ElemTypes[0].IsOrdered()
	if orderByValue {
		return indexOfOrderedSetValue
	}

	return indexOfSetValue
}

func indexOfSetValue(m []Value, v Value) int {
	return sort.Search(len(m), func(i int) bool {
		return !m[i].Ref().Less(v.Ref())
	})
}

func indexOfOrderedSetValue(m []Value, v Value) int {
	ov := v.(OrderedValue)

	return sort.Search(len(m), func(i int) bool {
		return !m[i].(OrderedValue).Less(ov)
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
			if !isSequenceOrderedByIndexedType(t) {
				indexValue = NewTypedRefFromValue(indexValue)
			}
		}

		return newMetaTuple(indexValue, set, NewTypedRefFromValue(set), uint64(len(items))), set
	}
}
