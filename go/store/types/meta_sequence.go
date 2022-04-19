// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/utils/tracing"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

var emptyKey = orderedKey{}

func newMetaTuple(ref Ref, key orderedKey, numLeaves uint64) (metaTuple, error) {
	d.PanicIfTrue(ref.buff == nil)
	w := newBinaryNomsWriter()
	var offsets [metaTuplePartNumLeaves + 1]uint32
	offsets[metaTuplePartRef] = w.offset
	err := ref.writeTo(&w, ref.format())

	if err != nil {
		return metaTuple{}, err
	}

	offsets[metaTuplePartKey] = w.offset
	err = key.writeTo(&w, ref.format())

	if err != nil {
		return metaTuple{}, err
	}

	offsets[metaTuplePartNumLeaves] = w.offset
	w.writeCount(numLeaves)
	return metaTuple{w.data(), offsets, ref.format()}, nil
}

// metaTuple is a node in a Prolly Tree, consisting of data in the node (either tree leaves or other metaSequences), and a Value annotation for exploring the tree (e.g. the largest item if this an ordered sequence).
type metaTuple struct {
	buff    []byte
	offsets [metaTuplePartNumLeaves + 1]uint32
	nbf     *NomsBinFormat
}

const (
	metaTuplePartRef       = 0
	metaTuplePartKey       = 1
	metaTuplePartNumLeaves = 2
)

func (mt metaTuple) decoderAtPart(part uint32, vrw ValueReadWriter) valueDecoder {
	offset := mt.offsets[part] - mt.offsets[metaTuplePartRef]
	return newValueDecoder(mt.buff[offset:], vrw)
}

func (mt metaTuple) ref() (Ref, error) {
	dec := mt.decoderAtPart(metaTuplePartRef, nil)
	return dec.readRef(mt.nbf)
}

func (mt metaTuple) key(vrw ValueReadWriter) (orderedKey, error) {
	dec := mt.decoderAtPart(metaTuplePartKey, vrw)
	return dec.readOrderedKey(mt.nbf)
}

func (mt metaTuple) numLeaves() uint64 {
	dec := mt.decoderAtPart(metaTuplePartNumLeaves, nil)
	return dec.readCount()
}

func (mt metaTuple) getChildSequence(ctx context.Context, vr ValueReader) (sequence, error) {
	ref, err := mt.ref()

	if err != nil {
		return nil, err
	}

	val, err := ref.TargetValue(ctx, vr)

	if err != nil {
		return nil, err
	}

	return val.(Collection).asSequence(), nil
}

func (mt metaTuple) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	w.writeRaw(mt.buff)
	return nil
}

// orderedKey is a key in a Prolly Tree level, which is a metaTuple in a metaSequence, or a value in a leaf sequence.
// |v| may be nil or |h| may be empty, but not both.
type orderedKey struct {
	isOrderedByValue bool
	v                Value
	h                hash.Hash
}

func newOrderedKey(v Value, nbf *NomsBinFormat) (orderedKey, error) {
	if isKindOrderedByValue(v.Kind()) {
		return orderedKey{true, v, hash.Hash{}}, nil
	}
	h, err := v.Hash(nbf)

	if err != nil {
		return orderedKey{}, err
	}

	return orderedKey{false, v, h}, nil
}

func orderedKeyFromHash(h hash.Hash) orderedKey {
	return orderedKey{false, nil, h}
}

func orderedKeyFromInt(n int, nbf *NomsBinFormat) (orderedKey, error) {
	return newOrderedKey(Float(n), nbf)
}

func orderedKeyFromUint64(n uint64, nbf *NomsBinFormat) (orderedKey, error) {
	return newOrderedKey(Float(n), nbf)
}

func (key orderedKey) Less(nbf *NomsBinFormat, mk2 orderedKey) (bool, error) {
	switch {
	case key.isOrderedByValue && mk2.isOrderedByValue:
		return key.v.Less(nbf, mk2.v)
	case key.isOrderedByValue:
		return true, nil
	case mk2.isOrderedByValue:
		return false, nil
	default:
		d.PanicIfTrue(key.h.IsEmpty() || mk2.h.IsEmpty())
		return key.h.Less(mk2.h), nil
	}
}

func (key orderedKey) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	if !key.isOrderedByValue {
		d.PanicIfTrue(key != emptyKey && key.h.IsEmpty())
		err := hashKind.writeTo(w, nbf)

		if err != nil {
			return err
		}

		w.writeHash(key.h)
	} else {
		err := key.v.writeTo(w, nbf)

		if err != nil {
			return err
		}
	}

	return nil
}

type metaSequence struct {
	sequenceImpl
}

func newMetaSequence(vrw ValueReadWriter, buff []byte, offsets []uint32, len uint64) metaSequence {
	return metaSequence{newSequenceImpl(vrw, buff, offsets, len)}
}

func newMetaSequenceFromTuples(kind NomsKind, level uint64, tuples []metaTuple, vrw ValueReadWriter) (metaSequence, error) {
	d.PanicIfFalse(level > 0)
	w := newBinaryNomsWriter()
	offsets := make([]uint32, len(tuples)+sequencePartValues+1)
	offsets[sequencePartKind] = w.offset
	err := kind.writeTo(&w, vrw.Format())

	if err != nil {
		return metaSequence{}, err
	}
	offsets[sequencePartLevel] = w.offset
	w.writeCount(level)
	offsets[sequencePartCount] = w.offset
	w.writeCount(uint64(len(tuples)))
	offsets[sequencePartValues] = w.offset
	length := uint64(0)
	for i, mt := range tuples {
		length += mt.numLeaves()
		err := mt.writeTo(&w, vrw.Format())

		if err != nil {
			return metaSequence{}, err
		}

		offsets[i+sequencePartValues+1] = w.offset
	}

	return newMetaSequence(vrw, w.data(), offsets, length), nil
}

func (ms metaSequence) tuples() ([]metaTuple, error) {
	dec, count := ms.decoderSkipToValues()
	tuples := make([]metaTuple, count)
	for i := uint64(0); i < count; i++ {
		var err error
		tuples[i], err = ms.readTuple(&dec)

		if err != nil {
			return nil, err
		}
	}

	return tuples, nil
}

func (ms metaSequence) getKey(idx int) (orderedKey, error) {
	dec := ms.decoderSkipToIndex(idx)
	err := dec.SkipValue(ms.format()) // ref

	if err != nil {
		return orderedKey{}, err
	}

	return dec.readOrderedKey(ms.format())
}

func (ms metaSequence) search(key orderedKey) (int, error) {
	res, err := SearchWithErroringLess(int(ms.seqLen()), func(i int) (bool, error) {
		ordKey, err := ms.getKey(i)

		if err != nil {
			return false, err
		}

		isLess, err := ordKey.Less(ms.format(), key)

		if err != nil {
			return false, err
		}

		return !isLess, nil
	})

	if err != nil {
		return 0, err
	}

	return res, nil
}

func (ms metaSequence) cumulativeNumberOfLeaves(idx int) (uint64, error) {
	cum := uint64(0)
	dec, _ := ms.decoderSkipToValues()
	for i := 0; i <= idx; i++ {
		err := dec.SkipValue(ms.format()) // ref

		if err != nil {
			return 0, err
		}

		err = dec.SkipValue(ms.format()) // v

		if err != nil {
			return 0, err
		}

		cum += dec.readCount()
	}
	return cum, nil
}

func (ms metaSequence) getCompareFn(other sequence) compareFn {
	dec := ms.decoder()
	oms := other.(metaSequence)
	otherDec := oms.decoder()
	return func(idx, otherIdx int) (bool, error) {
		msRef, err := ms.getRefAt(&dec, idx)

		if err != nil {
			return false, err
		}

		omsRef, err := oms.getRefAt(&otherDec, otherIdx)

		if err != nil {
			return false, err
		}

		return msRef.TargetHash() == omsRef.TargetHash(), nil
	}
}

func (ms metaSequence) readTuple(dec *valueDecoder) (metaTuple, error) {
	var offsets [metaTuplePartNumLeaves + 1]uint32
	start := dec.offset
	offsets[metaTuplePartRef] = start
	err := dec.skipRef()

	if err != nil {
		return metaTuple{}, err
	}

	offsets[metaTuplePartKey] = dec.offset
	err = dec.skipOrderedKey(ms.format())

	if err != nil {
		return metaTuple{}, err
	}

	offsets[metaTuplePartNumLeaves] = dec.offset
	dec.skipCount()
	end := dec.offset
	return metaTuple{dec.byteSlice(start, end), offsets, ms.format()}, nil
}

func (ms metaSequence) getRefAt(dec *valueDecoder, idx int) (Ref, error) {
	dec.offset = uint32(ms.getItemOffset(idx))
	return dec.readRef(ms.format())
}

func (ms metaSequence) getNumLeavesAt(idx int) (uint64, error) {
	dec := ms.decoderSkipToIndex(idx)
	err := dec.SkipValue(ms.format())

	if err != nil {
		return 0, err
	}

	err = dec.skipOrderedKey(ms.format())

	if err != nil {
		return 0, err
	}

	return dec.readCount(), nil
}

// sequence interface
func (ms metaSequence) getItem(idx int) (sequenceItem, error) {
	dec := ms.decoderSkipToIndex(idx)
	return ms.readTuple(&dec)
}

func (ms metaSequence) valuesSlice(from, to uint64) ([]Value, error) {
	panic("meta sequence")
}

func (seq metaSequence) kvTuples(from, to uint64, dest []Tuple) ([]Tuple, error) {
	panic("meta sequence")
}

func (ms metaSequence) typeOf() (*Type, error) {
	dec, count := ms.decoderSkipToValues()
	ts := make(typeSlice, 0, count)
	var lastRef Ref
	for i := uint64(0); i < count; i++ {
		ref, err := dec.readRef(ms.format())

		if err != nil {
			return nil, err
		}

		if lastRef.IsZeroValue() || !lastRef.isSameTargetType(ref) {
			lastRef = ref
			t, err := ref.TargetType()

			if err != nil {
				return nil, err
			}

			ts = append(ts, t)
		}

		err = dec.skipOrderedKey(ms.format()) // key

		if err != nil {
			return nil, err
		}

		dec.skipCount() // numLeaves
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
func (ms metaSequence) getChildSequence(ctx context.Context, idx int) (sequence, error) {
	span, ctx := tracing.StartSpan(ctx, "metaSequence.getChildSequence")
	defer func() {
		span.Finish()
	}()

	item, err := ms.getItem(idx)

	if err != nil {
		return nil, err
	}

	mt := item.(metaTuple)
	// TODO: IsZeroValue?
	if mt.buff == nil {
		return nil, nil
	}
	return mt.getChildSequence(ctx, ms.vrw)
}

// Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
// concatentation as one long composite sequence
func (ms metaSequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) (sequence, error) {
	span, ctx := tracing.StartSpan(ctx, "metaSequence.getChildSequence")
	span.LogKV("level", ms.treeLevel(), "length", length)
	defer func() {
		span.Finish()
	}()

	level := ms.treeLevel()
	d.PanicIfFalse(level > 0)
	if length == 0 {
		return emptySequence{level - 1, ms.format()}, nil
	}

	output, err := ms.getChildren(ctx, start, start+length)

	if err != nil {
		return nil, err
	}

	if level > 1 {
		var metaItems []metaTuple
		for _, seq := range output {
			tups, err := seq.(metaSequence).tuples()

			if err != nil {
				return nil, err
			}

			metaItems = append(metaItems, tups...)
		}

		return newMetaSequenceFromTuples(ms.Kind(), level-1, metaItems, ms.vrw)
	}

	switch ms.Kind() {
	case ListKind:
		var valueItems []Value
		for _, seq := range output {
			vals, err := seq.(listLeafSequence).values()

			if err != nil {
				return nil, err
			}

			valueItems = append(valueItems, vals...)
		}
		return newListLeafSequence(ms.vrw, valueItems...)
	case MapKind:
		var valueItems []mapEntry

		for _, seq := range output {
			entries, err := seq.(mapLeafSequence).entries()

			if err != nil {
				return nil, err
			}

			valueItems = append(valueItems, entries.entries...)
		}

		return newMapEntrySequence(ms.vrw, valueItems...)

	case SetKind:
		var valueItems []Value
		for _, seq := range output {
			vals, err := seq.(setLeafSequence).values()

			if err != nil {
				return nil, err
			}

			valueItems = append(valueItems, vals...)
		}

		return newSetLeafSequence(ms.vrw, valueItems...)
	}

	panic("unreachable")
}

// fetches child sequences from start (inclusive) to end (exclusive).
func (ms metaSequence) getChildren(ctx context.Context, start, end uint64) ([]sequence, error) {
	d.Chk.True(end <= uint64(ms.seqLen()))
	d.Chk.True(start <= end)

	seqs := make([]sequence, end-start)
	hs := make(hash.HashSlice, len(seqs))

	dec := ms.decoder()

	for i := start; i < end; i++ {
		ref, err := ms.getRefAt(&dec, int(i))

		if err != nil {
			return nil, err
		}

		hs[i-start] = ref.TargetHash()
	}

	if len(hs) == 0 {
		return seqs, nil // can occur with ptree that is fully uncommitted
	}

	// Fetch committed child sequences in a single batch
	readValues, err := ms.vrw.ReadManyValues(ctx, hs)

	if err != nil {
		return nil, err
	}

	for i, v := range readValues {
		col, ok := v.(Collection)
		if !ok {
			return nil, fmt.Errorf("corrupted database; nil where child collection .(Collection) should be; meta_sequence; i: %v, h: %v", i, hs[i])
		}
		seqs[i] = col.asSequence()
	}

	return seqs, err
}

func metaHashValueBytes(item sequenceItem, sp sequenceSplitter) error {
	return sp.Append(func(bw *binaryNomsWriter) error {
		bw.writeRaw(item.(metaTuple).buff)
		return nil
	})
}

type emptySequence struct {
	level uint64
	nbf   *NomsBinFormat
}

func (es emptySequence) getItem(idx int) (sequenceItem, error) {
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

func (es emptySequence) format() *NomsBinFormat {
	return es.nbf
}

func (es emptySequence) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (es emptySequence) getCompareFn(other sequence) compareFn {
	return func(idx, otherIdx int) (bool, error) { panic("empty sequence") }
}

func (es emptySequence) getKey(idx int) (orderedKey, error) {
	panic("empty sequence")
}

func (es emptySequence) search(key orderedKey) (int, error) {
	panic("empty sequence")
}

func (es emptySequence) cumulativeNumberOfLeaves(idx int) (uint64, error) {
	panic("empty sequence")
}

func (es emptySequence) getChildSequence(ctx context.Context, i int) (sequence, error) {
	return nil, nil
}

func (es emptySequence) Kind() NomsKind {
	panic("empty sequence")
}

func (es emptySequence) typeOf() (*Type, error) {
	panic("empty sequence")
}

func (es emptySequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) (sequence, error) {
	d.PanicIfFalse(es.level > 0)
	d.PanicIfFalse(start == 0)
	d.PanicIfFalse(length == 0)
	return emptySequence{es.level - 1, es.format()}, nil
}

func (es emptySequence) treeLevel() uint64 {
	return es.level
}

func (es emptySequence) isLeaf() bool {
	return es.level == 0
}

func (es emptySequence) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	panic("empty sequence")
}

func (es emptySequence) Equals(other Value) bool {
	panic("empty sequence")
}

func (es emptySequence) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	panic("empty sequence")
}

func (es emptySequence) Compare(nbf *NomsBinFormat, other LesserValuable) (int, error) {
	panic("empty sequence")
}

func (es emptySequence) valuesSlice(from, to uint64) ([]Value, error) {
	panic("empty sequence")
}

func (es emptySequence) kvTuples(from, to uint64, dest []Tuple) ([]Tuple, error) {
	panic("empty sequence")
}

func (es emptySequence) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
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
