// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"sort"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

const (
	objectWindowSize          = 8
	orderedSequenceWindowSize = 1
	objectPattern             = uint32(1<<6 - 1) // Average size of 64 elements
)

var emptyKey = orderedKey{}

func newMetaTuple(ref Ref, key orderedKey, numLeaves uint64) metaTuple {
	d.PanicIfTrue(ref.buff == nil)
	return metaTuple{ref, key, numLeaves}
}

// metaTuple is a node in a Prolly Tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	ref       Ref
	key       orderedKey
	numLeaves uint64
}

func (mt metaTuple) getChildSequence(vr ValueReader) sequence {
	return mt.ref.TargetValue(vr).(Collection).sequence()
}

func (mt metaTuple) writeTo(w nomsWriter) {
	mt.ref.writeTo(w)
	mt.key.writeTo(w)
	w.writeCount(mt.numLeaves)
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

func (key orderedKey) writeTo(w nomsWriter) {
	if !key.isOrderedByValue {
		// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
		d.PanicIfTrue(key.h.IsEmpty())
		writeRefPartsTo(w, key.h, BoolType, 0)
	} else {
		key.v.writeTo(w)
	}
}

type metaSequence struct {
	vrw     ValueReadWriter
	buff    []byte
	offsets []uint32
}

// readLeafSequence reads the data provided by a decoder and moves the decoder forward.
func readMetaSequence(dec *valueDecoder) metaSequence {
	start := dec.pos()
	offsets := skipMetaSequence(dec)
	end := dec.pos()
	return metaSequence{dec.vrw, dec.byteSlice(start, end), offsets}
}

func skipMetaSequence(dec *valueDecoder) []uint32 {
	kindPos := dec.pos()
	dec.skipKind()
	levelPos := dec.pos()
	dec.skipCount() // level
	countPos := dec.pos()
	count := dec.readCount()
	valuesPos := dec.pos()
	offsets := make([]uint32, count+sequencePartValues+1)
	offsets[sequencePartKind] = kindPos
	offsets[sequencePartLevel] = levelPos
	offsets[sequencePartCount] = countPos
	offsets[sequencePartValues] = valuesPos
	for i := uint64(0); i < count; i++ {
		dec.skipValue() // ref
		dec.skipValue() // v
		dec.skipCount() // numLeaves
		offsets[i+sequencePartValues+1] = dec.pos()
	}
	return offsets
}

func (ms metaSequence) writeTo(w nomsWriter) {
	w.writeRaw(ms.buff)
}

func (ms metaSequence) decoder() valueDecoder {
	return newValueDecoder(ms.buff, ms.vrw)
}

func (ms metaSequence) decoderAtOffset(offset int) valueDecoder {
	return newValueDecoder(ms.buff[offset:], ms.vrw)
}

func (ms metaSequence) decoderAtPart(part uint32) valueDecoder {
	offset := ms.offsets[part] - ms.offsets[sequencePartKind]
	return newValueDecoder(ms.buff[offset:], ms.vrw)
}

func (ms metaSequence) decoderSkipToValues() (valueDecoder, uint64) {
	dec := ms.decoderAtPart(sequencePartCount)
	count := dec.readCount()
	return dec, count
}

func (ms metaSequence) decoderSkipToIndex(idx int) valueDecoder {
	offset := ms.getItemOffset(idx)
	return ms.decoderAtOffset(offset)
}

func (ms metaSequence) getItemOffset(idx int) int {
	// kind, level, count, elements...
	// 0     1      2      3          n+1
	d.PanicIfTrue(idx+sequencePartValues+1 > len(ms.offsets))
	return int(ms.offsets[idx+sequencePartValues] - ms.offsets[sequencePartKind])
}

func newMetaSequence(kind NomsKind, level uint64, tuples []metaTuple, vrw ValueReadWriter) metaSequence {
	d.PanicIfFalse(level > 0)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(tuples)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	kind.writeTo(w)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(level)
	offsets[sequencePartCount] = w.offset
	w.writeCount(uint64(len(tuples)))
	offsets[sequencePartValues] = w.offset
	for i, mt := range tuples {
		mt.writeTo(w)
		offsets[i+sequencePartValues+1] = w.offset
	}
	return metaSequence{vrw, w.data(), offsets}
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
	dec.skipValue() // ref
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
		dec.skipValue() // ref
		dec.skipValue() // v
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
	ref := dec.readRef()
	key := dec.readOrderedKey()
	numLeaves := dec.readCount()
	return newMetaTuple(ref, key, numLeaves)
}

func (ms metaSequence) getRefAt(dec *valueDecoder, idx int) Ref {
	dec.offset = uint32(ms.getItemOffset(idx))
	return dec.readRef()
}

func (ms metaSequence) getNumLeavesAt(idx int) uint64 {
	dec := ms.decoderSkipToIndex(idx)
	dec.skipValue()
	dec.skipValue()
	return dec.readCount()
}

// sequence interface
func (ms metaSequence) getItem(idx int) sequenceItem {
	dec := ms.decoderSkipToIndex(idx)
	return ms.readTuple(&dec)
}

func (ms metaSequence) seqLen() int {
	_, count := ms.decoderSkipToValues()
	return int(count)
}

func (ms metaSequence) valueReadWriter() ValueReadWriter {
	return ms.vrw
}

func (ms metaSequence) hash() hash.Hash {
	return hash.Of(ms.buff)
}

func (ms metaSequence) equals(other sequence) bool {
	return bytes.Equal(ms.bytes(), other.bytes())
}

func (ms metaSequence) bytes() []byte {
	return ms.buff
}

func (ms metaSequence) WalkRefs(cb RefCallback) {
	dec, count := ms.decoderSkipToValues()
	for i := uint64(0); i < count; i++ {
		ref := dec.readRef()
		cb(ref)
		dec.skipValue() // v
		dec.skipCount() // numLeaves
	}
}

func (ms metaSequence) typeOf() *Type {
	dec, count := ms.decoderSkipToValues()
	ts := make(typeSlice, count)
	for i := uint64(0); i < count; i++ {
		ref := dec.readRef()
		ts[i] = ref.TargetType()
		dec.skipValue() // v
		dec.skipCount() // numLeaves
	}
	return makeCompoundType(UnionKind, ts...)
}

func (ms metaSequence) Kind() NomsKind {
	dec := ms.decoderAtPart(sequencePartKind)
	return dec.readKind()
}

func (ms metaSequence) numLeaves() uint64 {
	_, count := ms.decoderSkipToValues()
	return ms.cumulativeNumberOfLeaves(int(count - 1))
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
func (ms metaSequence) getChildSequence(idx int) sequence {
	mt := ms.getItem(idx).(metaTuple)
	// TODO: IsZeroValue?
	if mt.ref.buff == nil {
		return nil
	}
	return mt.getChildSequence(ms.vrw)
}

// Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
// concatentation as one long composite sequence
func (ms metaSequence) getCompositeChildSequence(start uint64, length uint64) sequence {
	level := ms.treeLevel()
	d.PanicIfFalse(level > 0)
	if length == 0 {
		return emptySequence{level - 1}
	}

	metaItems := []metaTuple{}
	mapItems := []mapEntry{}
	valueItems := []Value{}

	childIsMeta := false
	isIndexedSequence := false
	if ListKind == ms.Kind() {
		isIndexedSequence = true
	}

	// TODO: This looks strange. The children can only be a meta sequence or one of map/set/list
	// (why not blob?). We cannot mix map, set and list here and we know based on ms.Kind what
	// we are expecting.
	// https://github.com/attic-labs/noms/issues/3706
	output := ms.getChildren(start, start+length)
	for _, seq := range output {
		switch t := seq.(type) {
		case metaSequence:
			childIsMeta = true
			// TODO: Write directly to a valueEncoder
			metaItems = append(metaItems, t.tuples()...)
		case mapLeafSequence:
			mapItems = append(mapItems, t.entries()...)
		case setLeafSequence:
			valueItems = append(valueItems, t.values()...)
		case listLeafSequence:
			valueItems = append(valueItems, t.values()...)
		default:
			panic("unreachable")
		}
	}

	if childIsMeta {
		return newMetaSequence(ms.Kind(), ms.treeLevel()-1, metaItems, ms.vrw)
	}

	if isIndexedSequence {
		return newListLeafSequence(ms.vrw, valueItems...)
	}

	if MapKind == ms.Kind() {
		return newMapLeafSequence(ms.vrw, mapItems...)
	}

	return newSetLeafSequence(ms.vrw, valueItems...)
}

// fetches child sequences from start (inclusive) to end (exclusive).
func (ms metaSequence) getChildren(start, end uint64) (seqs []sequence) {
	d.Chk.True(end <= uint64(ms.seqLen()))
	d.Chk.True(start <= end)

	seqs = make([]sequence, end-start)
	hs := make(hash.HashSet, len(seqs))

	dec := ms.decoder()

	for i := start; i < end; i++ {
		hs[ms.getRefAt(&dec, int(i)).TargetHash()] = struct{}{}
	}

	if len(hs) == 0 {
		return // can occur with ptree that is fully uncommitted
	}

	// Fetch committed child sequences in a single batch
	valueChan := make(chan Value, len(hs))
	go func() {
		ms.vrw.ReadManyValues(hs, valueChan)
		close(valueChan)
	}()
	children := make(map[hash.Hash]sequence, len(hs))
	for value := range valueChan {
		children[value.Hash()] = value.(Collection).sequence()
	}

	for i := start; i < end; i++ {
		childSeq := children[ms.getRefAt(&dec, int(i)).TargetHash()]
		d.Chk.NotNil(childSeq)
		seqs[i-start] = childSeq
	}

	return
}

func metaHashValueBytes(item sequenceItem, rv *rollingValueHasher) {
	mt := item.(metaTuple)
	v := mt.key.v
	if !mt.key.isOrderedByValue {
		// See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
		d.PanicIfTrue(mt.key.h.IsEmpty())
		v = constructRef(mt.key.h, BoolType, 0)
	}

	hashValueBytes(mt.ref, rv)
	hashValueBytes(v, rv)
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

func (es emptySequence) getValue(idx int) Value {
	panic("empty sequence")
}

func (es emptySequence) cumulativeNumberOfLeaves(idx int) uint64 {
	panic("empty sequence")
}

func (es emptySequence) getChildSequence(i int) sequence {
	return nil
}

func (es emptySequence) Kind() NomsKind {
	panic("empty sequence")
}

func (es emptySequence) typeOf() *Type {
	panic("empty sequence")
}

func (es emptySequence) getCompositeChildSequence(start uint64, length uint64) sequence {
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

func (es emptySequence) hash() hash.Hash {
	panic("empty sequence")
}

func (es emptySequence) equals(other sequence) bool {
	panic("empty sequence")
}

func (es emptySequence) bytes() []byte {
	panic("empty sequence")
}

func (es emptySequence) writeTo(nomsWriter) {
	panic("empty sequence")
}
