// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

const (
	objectWindowSize          = 8
	orderedSequenceWindowSize = 1
	objectPattern             = uint32(1<<6 - 1) // Average size of 64 elements
)

var emptyKey = orderedKey{}

// metaSequence is a logical abstraction, but has no concrete "base" implementation. A Meta Sequence is a non-leaf (internal) node of a Prolly Tree, which results from the chunking of an ordered or unordered sequence of values.
type metaSequence interface {
	sequence
	getChildSequence(idx int) sequence
}

func newMetaTuple(ref Ref, key orderedKey, numLeaves uint64, child Collection) metaTuple {
	d.Chk.True(Ref{} != ref)
	return metaTuple{ref, key, numLeaves, child}
}

// metaTuple is a node in a Prolly Tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	ref       Ref
	key       orderedKey
	numLeaves uint64
	child     Collection // may be nil
}

// orderedKey is a key in a Prolly Tree level, which is a metaTuple in a metaSequence, or a value in a leaf sequence.
// |v| may be nil or |h| may be empty, but not both.
type orderedKey struct {
	isOrderedByValue bool
	v                Value
	h                hash.Hash
}

func newOrderedKey(v Value) orderedKey {
	d.Chk.NotNil(v)
	if isKindOrderedByValue(v.Type().Kind()) {
		return orderedKey{true, v, hash.Hash{}}
	}
	return orderedKey{false, v, v.Hash()}
}

func orderedKeyFromHash(h hash.Hash) orderedKey {
	return orderedKey{false, nil, h}
}

func orderedKeyFromInt(n int) orderedKey {
	return newOrderedKey(Number(n))
}

func orderedKeyFromUint64(n uint64) orderedKey {
	return newOrderedKey(Number(n))
}

func (key orderedKey) uint64Value() uint64 {
	return uint64(key.v.(Number))
}

func (key orderedKey) Less(mk2 orderedKey) bool {
	switch {
	case key.isOrderedByValue && mk2.isOrderedByValue:
		return key.v.Less(mk2.v)
	case key.isOrderedByValue:
		return true
	case mk2.isOrderedByValue:
		return false
	default:
		d.Chk.False(key.h.IsEmpty() || mk2.h.IsEmpty())
		return key.h.Less(mk2.h)
	}
}

type metaSequenceData []metaTuple

func (msd metaSequenceData) last() metaTuple {
	return msd[len(msd)-1]
}

type metaSequenceObject struct {
	tuples    metaSequenceData
	t         *Type
	vr        ValueReader
	leafCount uint64
}

func (ms metaSequenceObject) data() metaSequenceData {
	return ms.tuples
}

// sequence interface
func (ms metaSequenceObject) getItem(idx int) sequenceItem {
	return ms.tuples[idx]
}

func (ms metaSequenceObject) seqLen() int {
	return len(ms.tuples)
}

func (ms metaSequenceObject) valueReader() ValueReader {
	return ms.vr
}

func (ms metaSequenceObject) Chunks() []Ref {
	chunks := make([]Ref, len(ms.tuples))
	for i, tuple := range ms.tuples {
		chunks[i] = tuple.ref
	}
	return chunks
}

func (ms metaSequenceObject) Type() *Type {
	return ms.t
}

func (ms metaSequenceObject) numLeaves() uint64 {
	return ms.leafCount
}

// metaSequence interface
func (ms metaSequenceObject) getChildSequence(idx int) sequence {
	mt := ms.tuples[idx]
	if mt.child != nil {
		return mt.child.sequence()
	}

	return mt.ref.TargetValue(ms.vr).(Collection).sequence()
}

// Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
// concatentation as one long composite sequence
func (ms metaSequenceObject) getCompositeChildSequence(start uint64, length uint64) indexedSequence {
	childIsMeta := false
	metaItems := []metaTuple{}
	valueItems := []Value{}
	for i := start; i < start+length; i++ {
		seq := ms.getChildSequence(int(i))
		if i == start {
			if idxSeq, ok := seq.(indexedSequence); ok {
				childIsMeta = isMetaSequence(idxSeq)
			}
		}
		if childIsMeta {
			childMs, _ := seq.(indexedMetaSequence)
			metaItems = append(metaItems, childMs.metaSequenceObject.tuples...)
		} else {
			if ll, ok := seq.(listLeafSequence); ok {
				valueItems = append(valueItems, ll.values...)
			}
		}
	}

	if childIsMeta {
		return newIndexedMetaSequence(metaItems, ms.Type(), ms.vr)
	} else {
		return newListLeafSequence(ms.vr, valueItems...)
	}
}

func isMetaSequence(seq sequence) bool {
	_, seqIsMeta := seq.(metaSequence)
	return seqIsMeta
}

// Creates a sequenceCursor pointing to the first metaTuple in a metaSequence, and returns that cursor plus the leaf Value referenced from that metaTuple.
func newMetaSequenceCursor(root metaSequence, vr ValueReader) (*sequenceCursor, Value) {
	d.Chk.True(root != nil)

	cursors := []*sequenceCursor{newSequenceCursor(nil, root, 0)}
	for {
		cursor := cursors[len(cursors)-1]
		val := readMetaTupleValue(cursor.current(), vr)
		if ms, ok := val.(metaSequence); ok {
			cursors = append(cursors, newSequenceCursor(cursor, ms, 0))
		} else {
			return cursor, val
		}
	}
}

func readMetaTupleValue(item sequenceItem, vr ValueReader) Value {
	mt := item.(metaTuple)
	if mt.child != nil {
		return mt.child
	}

	r := mt.ref.TargetHash()
	d.Chk.False(r.IsEmpty())
	return vr.ReadValue(r)
}

func iterateMetaSequenceLeaf(ms metaSequence, vr ValueReader, cb func(Value) bool) {
	cursor, v := newMetaSequenceCursor(ms, vr)
	for {
		if cb(v) || !cursor.advance() {
			return
		}

		v = readMetaTupleValue(cursor.current(), vr)
	}
}
