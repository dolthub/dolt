// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"sort"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

var emptyKey = orderedKey{}

func newMetaTuple(ref Ref, key orderedKey, numLeaves uint64) metaTuple {
	d.PanicIfTrue(ref.buff == nil)
	w := newBinaryNomsWriter()
	var offsets [metaTuplePartNumLeaves + 1]uint32
	offsets[metaTuplePartRef] = w.offset
	// TODO(binformat)
	ref.writeTo(&w, Format_7_18)
	offsets[metaTuplePartKey] = w.offset
	// TODO(binformat)
	key.writeTo(&w, Format_7_18)
	offsets[metaTuplePartNumLeaves] = w.offset
	w.writeCount(numLeaves)
	return metaTuple{w.data(), offsets}
}

// metaTuple is a node in a Prolly Tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	buff    []byte
	offsets [metaTuplePartNumLeaves + 1]uint32
}

const (
	metaTuplePartRef       = 0
	metaTuplePartKey       = 1
	metaTuplePartNumLeaves = 2
)

func (mt metaTuple) decoderAtPart(part uint32) valueDecoder {
	offset := mt.offsets[part] - mt.offsets[metaTuplePartRef]
	return newValueDecoder(mt.buff[offset:], nil)
}

func (mt metaTuple) ref() Ref {
	dec := mt.decoderAtPart(metaTuplePartRef)
	return dec.readRef()
}

func (mt metaTuple) key() orderedKey {
	dec := mt.decoderAtPart(metaTuplePartKey)
	return dec.readOrderedKey()
}

func (mt metaTuple) numLeaves() uint64 {
	dec := mt.decoderAtPart(metaTuplePartNumLeaves)
	return dec.readCount()
}

func (mt metaTuple) getChildSequence(ctx context.Context, vr ValueReader) sequence {
	return mt.ref().TargetValue(ctx, vr).(Collection).asSequence()
}

func (mt metaTuple) writeTo(w nomsWriter, f *format) {
	w.writeRaw(mt.buff)
}

// orderedKey is a key in a Prolly Tree level, which is a metaTuple in a metaSequence, or a value in a leaf sequence.
// |v| may be nil or |h| may be empty, but not both.
type orderedKey struct {
	isOrderedByValue bool
	v                Value
	h                hash.Hash
}

func newOrderedKey(v Value) orderedKey {
	if isKindOrderedByValue(v.Kind()) {
		return orderedKey{true, v, hash.Hash{}}
	}
	// TODO(binformat)
	return orderedKey{false, v, v.Hash(Format_7_18)}
}

func orderedKeyFromHash(h hash.Hash) orderedKey {
	return orderedKey{false, nil, h}
}

func orderedKeyFromInt(n int) orderedKey {
	return newOrderedKey(Float(n))
}

func orderedKeyFromUint64(n uint64) orderedKey {
	return newOrderedKey(Float(n))
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

func (key orderedKey) writeTo(w nomsWriter, f *format) {
	if !key.isOrderedByValue {
		d.PanicIfTrue(key != emptyKey && key.h.IsEmpty())
		// TODO(binformat)
		hashKind.writeTo(w, Format_7_18)
		w.writeHash(key.h)
	} else {
		key.v.writeTo(w, f)
	}
}

type metaSequence struct {
	sequenceImpl
}

func newMetaSequence(vrw ValueReadWriter, buff []byte, offsets []uint32, len uint64) metaSequence {
	return metaSequence{newSequenceImpl(vrw, buff, offsets, len)}
}

func newMetaSequenceFromTuples(kind NomsKind, level uint64, tuples []metaTuple, vrw ValueReadWriter) metaSequence {
	d.PanicIfFalse(level > 0)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(tuples)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	// TODO(binformat)
	kind.writeTo(&w, Format_7_18)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(level)
	offsets[sequencePartCount] = w.offset
	w.writeCount(uint64(len(tuples)))
	offsets[sequencePartValues] = w.offset
	length := uint64(0)
	for i, mt := range tuples {
		length += mt.numLeaves()
		// TODO(binformat)
		mt.writeTo(&w, Format_7_18)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return newMetaSequence(vrw, w.data(), offsets, length)
}

func (ms metaSequence) tuples() []metaTuple {
	dec, count := ms.decoderSkipToValues()
	tuples := make([]metaTuple, count)
	for i := uint64(0); i < count; i++ {
		tuples[i] = ms.readTuple(&dec)
	}
	return tuples
}

func (ms metaSequence) getKey(idx int) orderedKey {
	dec := ms.decoderSkipToIndex(idx)
	// TODO(binformat)
	dec.skipValue(Format_7_18) // ref
	return dec.readOrderedKey()
}

func (ms metaSequence) search(key orderedKey) int {
	return sort.Search(ms.seqLen(), func(i int) bool {
		return !ms.getKey(i).Less(key)
	})
}

func (ms metaSequence) cumulativeNumberOfLeaves(idx int) uint64 {
	cum := uint64(0)
	dec, _ := ms.decoderSkipToValues()
	for i := 0; i <= idx; i++ {
		// TODO(binformat)
		dec.skipValue(Format_7_18) // ref
		dec.skipValue(Format_7_18) // v
		cum += dec.readCount()
	}
	return cum
}

func (ms metaSequence) getCompareFn(other sequence) compareFn {
	dec := ms.decoder()
	oms := other.(metaSequence)
	otherDec := oms.decoder()
	return func(idx, otherIdx int) bool {
		return ms.getRefAt(&dec, idx).TargetHash() == oms.getRefAt(&otherDec, otherIdx).TargetHash()
	}
}

func (ms metaSequence) readTuple(dec *valueDecoder) metaTuple {
	var offsets [metaTuplePartNumLeaves + 1]uint32
	start := dec.offset
	offsets[metaTuplePartRef] = start
	dec.skipRef()
	offsets[metaTuplePartKey] = dec.offset
	dec.skipOrderedKey()
	offsets[metaTuplePartNumLeaves] = dec.offset
	dec.skipCount()
	end := dec.offset
	return metaTuple{dec.byteSlice(start, end), offsets}
}

func (ms metaSequence) getRefAt(dec *valueDecoder, idx int) Ref {
	dec.offset = uint32(ms.getItemOffset(idx))
	return dec.readRef()
}

func (ms metaSequence) getNumLeavesAt(idx int) uint64 {
	dec := ms.decoderSkipToIndex(idx)
	// TODO(binformat)
	dec.skipValue(Format_7_18)
	dec.skipOrderedKey()
	return dec.readCount()
}

// sequence interface
func (ms metaSequence) getItem(idx int) sequenceItem {
	dec := ms.decoderSkipToIndex(idx)
	return ms.readTuple(&dec)
}

func (ms metaSequence) valuesSlice(f *format, from, to uint64) []Value {
	panic("meta sequence")
}

func (ms metaSequence) typeOf() *Type {
	dec, count := ms.decoderSkipToValues()
	ts := make(typeSlice, 0, count)
	var lastRef Ref
	for i := uint64(0); i < count; i++ {
		ref := dec.readRef()
		if lastRef.IsZeroValue() || !lastRef.isSameTargetType(ref) {
			lastRef = ref
			t := ref.TargetType()
			ts = append(ts, t)
		}

		dec.skipOrderedKey() // key
		dec.skipCount()      // numLeaves
	}

	return makeUnionType(ts...)
}

func (ms metaSequence) numLeaves() uint64 {
	return ms.len
}

func (ms metaSequence) treeLevel() uint64 {
	dec := ms.decoderAtPart(sequencePartLevel)
	return dec.readCount()
}

func (ms metaSequence) isLeaf() bool {
	d.PanicIfTrue(ms.treeLevel() == 0)
	return false
}

// metaSequence interface
func (ms metaSequence) getChildSequence(ctx context.Context, idx int) sequence {
	mt := ms.getItem(idx).(metaTuple)
	// TODO: IsZeroValue?
	if mt.buff == nil {
		return nil
	}
	return mt.getChildSequence(ctx, ms.vrw)
}

// Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
// concatentation as one long composite sequence
func (ms metaSequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) sequence {
	level := ms.treeLevel()
	d.PanicIfFalse(level > 0)
	if length == 0 {
		return emptySequence{level - 1}
	}

	output := ms.getChildren(ctx, start, start+length)

	if level > 1 {
		var metaItems []metaTuple
		for _, seq := range output {
			metaItems = append(metaItems, seq.(metaSequence).tuples()...)
		}
		return newMetaSequenceFromTuples(ms.Kind(), level-1, metaItems, ms.vrw)
	}

	switch ms.Kind() {
	case ListKind:
		var valueItems []Value
		for _, seq := range output {
			valueItems = append(valueItems, seq.(listLeafSequence).values()...)
		}
		// TODO(binformat)
		return newListLeafSequence(ms.vrw, Format_7_18, valueItems...)
	case MapKind:
		var valueItems []mapEntry
		for _, seq := range output {
			valueItems = append(valueItems, seq.(mapLeafSequence).entries()...)
		}
		return newMapLeafSequence(ms.vrw, valueItems...)
	case SetKind:
		var valueItems []Value
		for _, seq := range output {
			valueItems = append(valueItems, seq.(setLeafSequence).values()...)
		}
		return newSetLeafSequence(ms.vrw, valueItems...)
	}

	panic("unreachable")
}

// fetches child sequences from start (inclusive) to end (exclusive).
func (ms metaSequence) getChildren(ctx context.Context, start, end uint64) (seqs []sequence) {
	d.Chk.True(end <= uint64(ms.seqLen()))
	d.Chk.True(start <= end)

	seqs = make([]sequence, end-start)
	hs := make(hash.HashSlice, len(seqs))

	dec := ms.decoder()

	for i := start; i < end; i++ {
		hs[i-start] = ms.getRefAt(&dec, int(i)).TargetHash()
	}

	if len(hs) == 0 {
		return // can occur with ptree that is fully uncommitted
	}

	// Fetch committed child sequences in a single batch
	readValues := ms.vrw.ReadManyValues(ctx, hs)
	for i, v := range readValues {
		seqs[i] = v.(Collection).asSequence()
	}

	return
}

func metaHashValueBytes(item sequenceItem, rv *rollingValueHasher) {
	rv.hashBytes(item.(metaTuple).buff)
}

type emptySequence struct {
	level uint64
}

func (es emptySequence) getItem(idx int) sequenceItem {
	panic("empty sequence")
}

func (es emptySequence) seqLen() int {
	return 0
}

func (es emptySequence) numLeaves() uint64 {
	return 0
}

func (es emptySequence) valueReadWriter() ValueReadWriter {
	return nil
}

func (es emptySequence) WalkRefs(cb RefCallback) {
}

func (es emptySequence) getCompareFn(other sequence) compareFn {
	return func(idx, otherIdx int) bool { panic("empty sequence") }
}

func (es emptySequence) getKey(idx int) orderedKey {
	panic("empty sequence")
}

func (es emptySequence) search(key orderedKey) int {
	panic("empty sequence")
}

func (es emptySequence) cumulativeNumberOfLeaves(idx int) uint64 {
	panic("empty sequence")
}

func (es emptySequence) getChildSequence(ctx context.Context, i int) sequence {
	return nil
}

func (es emptySequence) Kind() NomsKind {
	panic("empty sequence")
}

func (es emptySequence) typeOf() *Type {
	panic("empty sequence")
}

func (es emptySequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) sequence {
	d.PanicIfFalse(es.level > 0)
	d.PanicIfFalse(start == 0)
	d.PanicIfFalse(length == 0)
	return emptySequence{es.level - 1}
}

func (es emptySequence) treeLevel() uint64 {
	return es.level
}

func (es emptySequence) isLeaf() bool {
	return es.level == 0
}

func (es emptySequence) Hash(f *format) hash.Hash {
	panic("empty sequence")
}

func (es emptySequence) Equals(other Value) bool {
	panic("empty sequence")
}

func (es emptySequence) Less(other LesserValuable) bool {
	panic("empty sequence")
}

func (es emptySequence) valueBytes(*format) []byte {
	panic("empty sequence")
}

func (es emptySequence) valuesSlice(f *format, from, to uint64) []Value {
	panic("empty sequence")
}

func (es emptySequence) writeTo(w nomsWriter, f *format) {
	panic("empty sequence")
}

func (es emptySequence) Empty() bool {
	panic("empty sequence")
}

func (es emptySequence) Len() uint64 {
	panic("empty sequence")
}

func (es emptySequence) asValueImpl() valueImpl {
	panic("empty sequence")
}
