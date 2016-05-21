package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/hash"
)

const (
	// The window size to use for computing the rolling hash.
	setWindowSize = 1
	setPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type Set struct {
	seq orderedSequence
	h   *hash.Hash
}

func newSet(seq orderedSequence) Set {
	return Set{seq, &hash.Hash{}}
}

func NewSet(v ...Value) Set {
	data := buildSetData(v)
	seq := newEmptySequenceChunker(makeSetLeafChunkFn(nil), newOrderedMetaSequenceChunkFn(SetKind, nil), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)

	for _, v := range data {
		seq.Append(v)
	}

	return seq.Done().(Set)
}

func (s Set) Diff(last Set) (added []Value, removed []Value) {
	// Set diff shouldn't return modified since it's not possible a value in a set of "changes".
	// Elements can only enter and exit a set
	added, removed, _ = orderedSequenceDiff(last.sequence().(orderedSequence), s.sequence().(orderedSequence))
	return
}

// Collection interface
func (s Set) Len() uint64 {
	return s.seq.numLeaves()
}

func (s Set) Empty() bool {
	return s.Len() == 0
}

func (s Set) sequence() sequence {
	return s.seq
}

// Value interface
func (s Set) Equals(other Value) bool {
	return other != nil && s.Hash() == other.Hash()
}

func (s Set) Less(other Value) bool {
	return valueLess(s, other)
}

func (s Set) Hash() hash.Hash {
	return EnsureRef(s.h, s)
}

func (s Set) ChildValues() (values []Value) {
	s.IterAll(func(v Value) {
		values = append(values, v)
	})
	return
}

func (s Set) Chunks() []Ref {
	return s.seq.Chunks()
}

func (s Set) Type() *Type {
	return s.seq.Type()
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
	ch := newSequenceChunker(cur, makeSetLeafChunkFn(s.seq.valueReader()), newOrderedMetaSequenceChunkFn(SetKind, s.seq.valueReader()), newSetLeafBoundaryChecker(), newOrderedMetaSequenceBoundaryChecker)
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

func buildSetData(values ValueSlice) ValueSlice {
	if len(values) == 0 {
		return ValueSlice{}
	}

	uniqueSorted := make(ValueSlice, 0, len(values))
	sort.Stable(values)
	last := values[0]
	for i := 1; i < len(values); i++ {
		v := values[i]
		if !v.Equals(last) {
			uniqueSorted = append(uniqueSorted, last)
		}
		last = v
	}

	return append(uniqueSorted, last)
}

func newSetLeafBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(setWindowSize, sha1.Size, setPattern, func(item sequenceItem) []byte {
		digest := item.(Value).Hash().Digest()
		return digest[:]
	})
}

func makeSetLeafChunkFn(vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (metaTuple, Collection) {
		setData := make([]Value, len(items), len(items))

		for i, v := range items {
			setData[i] = v.(Value)
		}

		set := newSet(newSetLeafSequence(vr, setData...))

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
