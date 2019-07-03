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

func newMetaTuple(f *Format, ref Ref, key orderedKey, numLeaves uint64) metaTuple {
	d.PanicIfTrue(ref.buff == nil)
	w := newBinaryNomsWriter()
	var offsets [metaTuplePartNumLeaves + 1]uint32
	offsets[metaTuplePartRef] = w.offset
	ref.writeTo(&w, f)
	offsets[metaTuplePartKey] = w.offset
	key.writeTo(&w, f)
	offsets[metaTuplePartNumLeaves] = w.offset
	w.writeCount(numLeaves)
	return metaTuple{w.data(), offsets, f}
}

// metaTuple is a node in a Prolly Tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	buff    []byte
	offsets [metaTuplePartNumLeaves + 1]uint32
	format  *Format
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
	return dec.readRef(mt.format)
}

func (mt metaTuple) key() orderedKey {
	dec := mt.decoderAtPart(metaTuplePartKey)
	return dec.readOrderedKey(mt.format)
}

func (mt metaTuple) numLeaves() uint64 {
	dec := mt.decoderAtPart(metaTuplePartNumLeaves)
	return dec.readCount()
}

func (mt metaTuple) getChildSequence(ctx context.Context, vr ValueReader) sequence {
	return mt.ref().TargetValue(ctx, vr).(Collection).asSequence()
}

func (mt metaTuple) writeTo(w nomsWriter, f *Format) {
	w.writeRaw(mt.buff)
}

// orderedKey is a key in a Prolly Tree level, which is a metaTuple in a metaSequence, or a value in a leaf sequence.
// |v| may be nil or |h| may be empty, but not both.
type orderedKey struct {
	isOrderedByValue bool
	v                Value
	h                hash.Hash
}

func newOrderedKey(v Value, f *Format) orderedKey {
	if isKindOrderedByValue(v.Kind()) {
		return orderedKey{true, v, hash.Hash{}}
	}
	// TODO(binformat)
	return orderedKey{false, v, v.Hash(f)}
}

func orderedKeyFromHash(h hash.Hash) orderedKey {
	return orderedKey{false, nil, h}
}

func orderedKeyFromInt(n int, f *Format) orderedKey {
	return newOrderedKey(Float(n), f)
}

func orderedKeyFromUint64(n uint64, f *Format) orderedKey {
	return newOrderedKey(Float(n), f)
}

func (key orderedKey) Less(f *Format, mk2 orderedKey) bool {
	switch {
	case key.isOrderedByValue && mk2.isOrderedByValue:
		return key.v.Less(f, mk2.v)
	case key.isOrderedByValue:
		return true
	case mk2.isOrderedByValue:
		return false
	default:
		d.PanicIfTrue(key.h.IsEmpty() || mk2.h.IsEmpty())
		return key.h.Less(mk2.h)
	}
}

func (key orderedKey) writeTo(w nomsWriter, f *Format) {
	if !key.isOrderedByValue {
		d.PanicIfTrue(key != emptyKey && key.h.IsEmpty())
		hashKind.writeTo(w, f)
		w.writeHash(key.h)
	} else {
		key.v.writeTo(w, f)
	}
}

type metaSequence struct {
	sequenceImpl
}

func newMetaSequence(f *Format, vrw ValueReadWriter, buff []byte, offsets []uint32, len uint64) metaSequence {
	return metaSequence{newSequenceImpl(vrw, f, buff, offsets, len)}
}

func newMetaSequenceFromTuples(f *Format, kind NomsKind, level uint64, tuples []metaTuple, vrw ValueReadWriter) metaSequence {
	d.PanicIfFalse(level > 0)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(tuples)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	kind.writeTo(&w, f)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(level)
	offsets[sequencePartCount] = w.offset
	w.writeCount(uint64(len(tuples)))
	offsets[sequencePartValues] = w.offset
	length := uint64(0)
	for i, mt := range tuples {
		length += mt.numLeaves()
		mt.writeTo(&w, f)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return newMetaSequence(f, vrw, w.data(), offsets, length)
}

func (ms metaSequence) tuples() []metaTuple {
	dec, count := ms.decoderSkipToValues()
	tuples := make([]metaTuple, count)
	for i := uint64(0); i < count; i++ {
		tuples[i] = ms.readTuple(ms.format, &dec)
	}
	return tuples
}

func (ms metaSequence) getKey(idx int) orderedKey {
	dec := ms.decoderSkipToIndex(idx)
	dec.skipValue(ms.format) // ref
	return dec.readOrderedKey(ms.format)
}

func (ms metaSequence) search(key orderedKey) int {
	return sort.Search(ms.seqLen(), func(i int) bool {
		return !ms.getKey(i).Less(ms.format, key)
	})
}

func (ms metaSequence) cumulativeNumberOfLeaves(idx int) uint64 {
	cum := uint64(0)
	dec, _ := ms.decoderSkipToValues()
	for i := 0; i <= idx; i++ {
		dec.skipValue(ms.format) // ref
		dec.skipValue(ms.format) // v
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

func (ms metaSequence) readTuple(f *Format, dec *valueDecoder) metaTuple {
	var offsets [metaTuplePartNumLeaves + 1]uint32
	start := dec.offset
	offsets[metaTuplePartRef] = start
	dec.skipRef()
	offsets[metaTuplePartKey] = dec.offset
	dec.skipOrderedKey(f)
	offsets[metaTuplePartNumLeaves] = dec.offset
	dec.skipCount()
	end := dec.offset
	return metaTuple{dec.byteSlice(start, end), offsets, f}
}

func (ms metaSequence) getRefAt(dec *valueDecoder, idx int) Ref {
	dec.offset = uint32(ms.getItemOffset(idx))
	return dec.readRef(ms.format)
}

func (ms metaSequence) getNumLeavesAt(idx int) uint64 {
	dec := ms.decoderSkipToIndex(idx)
	dec.skipValue(ms.format)
	dec.skipOrderedKey(ms.format)
	return dec.readCount()
}

// sequence interface
func (ms metaSequence) getItem(idx int, f *Format) sequenceItem {
	dec := ms.decoderSkipToIndex(idx)
	return ms.readTuple(f, &dec)
}

func (ms metaSequence) valuesSlice(f *Format, from, to uint64) []Value {
	panic("meta sequence")
}

func (ms metaSequence) typeOf() *Type {
	dec, count := ms.decoderSkipToValues()
	ts := make(typeSlice, 0, count)
	var lastRef Ref
	for i := uint64(0); i < count; i++ {
		ref := dec.readRef(ms.format)
		if lastRef.IsZeroValue() || !lastRef.isSameTargetType(ref) {
			lastRef = ref
			t := ref.TargetType()
			ts = append(ts, t)
		}

		dec.skipOrderedKey(ms.format) // key
		dec.skipCount()               // numLeaves
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
	mt := ms.getItem(idx, ms.format).(metaTuple)
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
		return newMetaSequenceFromTuples(ms.format, ms.Kind(), level-1, metaItems, ms.vrw)
	}

	switch ms.Kind() {
	case ListKind:
		var valueItems []Value
		for _, seq := range output {
			valueItems = append(valueItems, seq.(listLeafSequence).values(ms.format)...)
		}
		return newListLeafSequence(ms.vrw, ms.format, valueItems...)
	case MapKind:
		var valueItems []mapEntry
		for _, seq := range output {
			valueItems = append(valueItems, seq.(mapLeafSequence).entries().entries...)
		}
		return newMapLeafSequence(ms.format, ms.vrw, valueItems...)
	case SetKind:
		var valueItems []Value
		for _, seq := range output {
			valueItems = append(valueItems, seq.(setLeafSequence).values(ms.format)...)
		}
		return newSetLeafSequence(ms.format, ms.vrw, valueItems...)
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

func (es emptySequence) getItem(idx int, f *Format) sequenceItem {
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

func (es emptySequence) WalkRefs(f *Format, cb RefCallback) {
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

func (es emptySequence) Hash(f *Format) hash.Hash {
	panic("empty sequence")
}

func (es emptySequence) Equals(other Value) bool {
	panic("empty sequence")
}

func (es emptySequence) Less(f *Format, other LesserValuable) bool {
	panic("empty sequence")
}

func (es emptySequence) valueBytes(*Format) []byte {
	panic("empty sequence")
}

func (es emptySequence) valuesSlice(f *Format, from, to uint64) []Value {
	panic("empty sequence")
}

func (es emptySequence) writeTo(w nomsWriter, f *Format) {
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
