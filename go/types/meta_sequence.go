// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/util/orderedparallel"
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
	d.PanicIfFalse(Ref{} != ref)
	return metaTuple{ref, key, numLeaves, child}
}

// metaTuple is a node in a Prolly Tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	ref       Ref
	key       orderedKey
	numLeaves uint64
	child     Collection // may be nil
}

func (mt metaTuple) getChildSequence(vr ValueReader) sequence {
	if mt.child != nil {
		return mt.child.sequence()
	}

	return mt.ref.TargetValue(vr).(Collection).sequence()
}

// orderedKey is a key in a Prolly Tree level, which is a metaTuple in a metaSequence, or a value in a leaf sequence.
// |v| may be nil or |h| may be empty, but not both.
type orderedKey struct {
	isOrderedByValue bool
	v                Value
	h                hash.Hash
}

func newOrderedKey(v Value) orderedKey {
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
		d.PanicIfTrue(key.h.IsEmpty() || mk2.h.IsEmpty())
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
	return mt.getChildSequence(ms.vr)
}

func (ms metaSequenceObject) beginFetchingChildSequences(start, length uint64) chan interface{} {
	input := make(chan interface{})
	output := orderedparallel.New(input, func(item interface{}) interface{} {
		i := item.(int)
		return ms.getChildSequence(i)
	}, int(length))

	go func() {
		for i := start; i < start+length; i++ {
			input <- int(i)
		}

		close(input)
	}()
	return output
}

// Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
// concatentation as one long composite sequence
func (ms metaSequenceObject) getCompositeChildSequence(start uint64, length uint64) sequence {
	if length == 0 {
		return emptySequence{}
	}

	metaItems := []metaTuple{}
	mapItems := []mapEntry{}
	valueItems := []Value{}

	childIsMeta := false
	isIndexedSequence := false
	if ListKind == ms.Type().Kind() {
		isIndexedSequence = true
	}

	output := ms.beginFetchingChildSequences(start, length)
	for item := range output {
		seq := item.(sequence)

		switch t := seq.(type) {
		case indexedMetaSequence:
			childIsMeta = true
			metaItems = append(metaItems, t.metaSequenceObject.tuples...)
		case orderedMetaSequence:
			childIsMeta = true
			metaItems = append(metaItems, t.metaSequenceObject.tuples...)
		case mapLeafSequence:
			mapItems = append(mapItems, t.data...)
		case setLeafSequence:
			valueItems = append(valueItems, t.data...)
		case listLeafSequence:
			valueItems = append(valueItems, t.values...)
		default:
			panic("unreachable")
		}
	}

	if isIndexedSequence {
		if childIsMeta {
			return newIndexedMetaSequence(metaItems, ms.Type(), ms.vr)
		}
		return newListLeafSequence(ms.vr, valueItems...)
	}
	if childIsMeta {
		return newOrderedMetaSequence(metaItems, ms.Type(), ms.vr)
	}
	if MapKind == ms.Type().Kind() {
		return newMapLeafSequence(ms.vr, mapItems...)
	}
	return newSetLeafSequence(ms.vr, valueItems...)
}

func isMetaSequence(seq sequence) bool {
	_, seqIsMeta := seq.(metaSequence)
	return seqIsMeta
}

func readMetaTupleValue(item sequenceItem, vr ValueReader) Value {
	mt := item.(metaTuple)
	if mt.child != nil {
		return mt.child
	}

	r := mt.ref.TargetHash()
	d.PanicIfTrue(r.IsEmpty())
	return vr.ReadValue(r)
}

func metaHashValueBytes(item sequenceItem, rv *rollingValueHasher) {
	mt := item.(metaTuple)
	v := mt.key.v
	if !mt.key.isOrderedByValue {
		// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
		d.PanicIfTrue(mt.key.h.IsEmpty())
		v = constructRef(MakeRefType(BoolType), mt.key.h, 0)
	}

	hashValueBytes(mt.ref, rv)
	hashValueBytes(v, rv)
}

type emptySequence struct{}

func (es emptySequence) getItem(idx int) sequenceItem {
	panic("empty sequence")
}

func (es emptySequence) seqLen() int {
	return 0
}

func (es emptySequence) numLeaves() uint64 {
	return 0
}

func (es emptySequence) valueReader() ValueReader {
	return nil
}

func (es emptySequence) Chunks() (chunks []Ref) {
	return
}

func (es emptySequence) Type() *Type {
	panic("empty sequence")
}

func (es emptySequence) getCompareFn(other sequence) compareFn {
	return func(idx, otherIdx int) bool { panic("empty sequence") }
}

func (es emptySequence) getKey(idx int) orderedKey {
	panic("empty sequence")
}

func (es emptySequence) cumulativeNumberOfLeaves(idx int) uint64 {
	panic("empty sequence")
}
