// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"fmt"
	"github.com/attic-labs/noms/go/d"
)

var EmptyTuple = NewTuple()

type TupleIterator struct {
	dec   valueDecoder
	count uint64
	pos   uint64
}

func (itr *TupleIterator) Next() (uint64, Value) {
	if itr.pos < itr.count {
		valPos := itr.pos
		val := itr.dec.readValue()

		itr.pos++
		return valPos, val
	}

	return itr.count, nil
}

func (itr *TupleIterator) HasMore() bool {
	return itr.pos < itr.count
}

func (itr *TupleIterator) Len() uint64 {
	return itr.count
}

func (itr *TupleIterator) Pos() uint64 {
	return itr.pos
}

type Tuple struct {
	valueImpl
}

// readTuple reads the data provided by a decoder and moves the decoder forward.
func readTuple(dec *valueDecoder) Tuple {
	start := dec.pos()
	skipTuple(dec)
	end := dec.pos()
	return Tuple{valueImpl{dec.vrw, dec.byteSlice(start, end), nil}}
}

func skipTuple(dec *valueDecoder) {
	dec.skipKind()
	count := dec.readCount()
	for i := uint64(0); i < count; i++ {
		dec.skipValue()
	}
}

func walkTuple(r *refWalker, cb RefCallback) {
	r.skipKind()
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.walkValue(cb)
	}
}

func NewTuple(values ...Value) Tuple {
	var vrw ValueReadWriter
	w := newBinaryNomsWriter()
	TupleKind.writeTo(&w)
	numVals := len(values)
	w.writeCount(uint64(numVals))
	for i := 0; i < numVals; i++ {
		if vrw == nil {
			vrw = values[i].(valueReadWriter).valueReadWriter()
		}
		values[i].writeTo(&w)
	}
	return Tuple{valueImpl{vrw, w.data(), nil}}
}

func (t Tuple) Empty() bool {
	return t.Len() == 0
}

// Value interface
func (t Tuple) Value(ctx context.Context) Value {
	return t
}

func (t Tuple) WalkValues(ctx context.Context, cb ValueCallback) {
	dec, count := t.decoderSkipToFields()
	for i := uint64(0); i < count; i++ {
		cb(dec.readValue())
	}
}

func (t Tuple) typeOf() *Type {
	dec, count := t.decoderSkipToFields()
	ts := make(typeSlice, 0, count)
	var lastType *Type
	for i := uint64(0); i < count; i++ {
		if lastType != nil {
			offset := dec.offset
			if dec.isValueSameTypeForSure(lastType) {
				continue
			}
			dec.offset = offset
		}

		lastType = dec.readTypeOfValue()
		ts = append(ts, lastType)
	}

	return makeCompoundType(TupleKind, makeUnionType(ts...))
}

func (t Tuple) decoderSkipToFields() (valueDecoder, uint64) {
	dec := t.decoder()
	dec.skipKind()
	count := dec.readCount()
	return dec, count
}

// Len is the number of fields in the struct.
func (t Tuple) Len() uint64 {
	_, count := t.decoderSkipToFields()
	return count
}

func (t Tuple) Iterator() *TupleIterator {
	return t.IteratorAt(0)
}

func (t Tuple) IteratorAt(pos uint64) *TupleIterator {
	dec, count := t.decoderSkipToFields()

	for i := uint64(0); i < pos; i++ {
		dec.skipValue()
	}

	return &TupleIterator{dec, count, pos}
}

// IterFields iterates over the fields, calling cb for every field in the tuple until cb returns false
func (t Tuple) IterFields(cb func(index uint64, value Value) (stop bool)) {
	itr := t.Iterator()
	for itr.HasMore() {
		i, curr := itr.Next()
		stop := cb(i, curr)

		if stop {
			break
		}
	}
}

// Get returns the value of a field in the tuple. If the tuple does not a have a field at the index then this panics
func (t Tuple) Get(n uint64) Value {
	dec, count := t.decoderSkipToFields()

	if n >= count {
		d.Chk.Fail(fmt.Sprintf(`tuple index "%d" out of range`, n))
	}

	for i := uint64(0); i < n; i++ {
		dec.skipValue()
	}

	v := dec.readValue()
	return v
}

// Set returns a new tuple where the field at index n is set to value. Attempting to use Set on an index that is outside
// of the bounds will cause a panic.  Use Append to add additional values, not Set.
func (t Tuple) Set(n uint64, v Value) Tuple {
	prolog, head, tail, count, found := t.splitFieldsAt(n)
	if !found {
		d.Panic("Cannot set tuple value at index %d as it is outside the range [0,%d]", n, count-1)
	}

	w := binaryNomsWriter{make([]byte, len(t.buff)), 0}
	w.writeRaw(prolog)

	w.writeCount(count)
	w.writeRaw(head)
	v.writeTo(&w)
	w.writeRaw(tail)

	return Tuple{valueImpl{t.vrw, w.data(), nil}}
}

func (t Tuple) Append(v Value) Tuple {
	dec := t.decoder()
	dec.skipKind()
	prolog := dec.buff[:dec.offset]
	count := dec.readCount()
	fieldsOffset := dec.offset

	w := binaryNomsWriter{make([]byte, len(t.buff)), 0}
	w.writeRaw(prolog)
	w.writeCount(count + 1)
	w.writeRaw(dec.buff[fieldsOffset:])
	v.writeTo(&w)

	return Tuple{valueImpl{t.vrw, w.data(), nil}}
}

// splitFieldsAt splits the buffer into two parts. The fields coming before the field we are looking for
// and the fields coming after it.
func (t Tuple) splitFieldsAt(n uint64) (prolog, head, tail []byte, count uint64, found bool) {
	dec := t.decoder()
	dec.skipKind()
	prolog = dec.buff[:dec.offset]
	count = dec.readCount()

	if n >= count {
		return nil, nil, nil, count, false
	}

	found = true
	fieldsOffset := dec.offset

	for i := uint64(0); i < n; i++ {
		dec.skipValue()
	}

	head = dec.buff[fieldsOffset:dec.offset]

	if n != count-1 {
		dec.skipValue()
		tail = dec.buff[dec.offset:len(dec.buff)]
	}

	return
}

/*func (t Tuple) Equals(otherVal Value) bool {
	if otherTuple, ok := otherVal.(Tuple); ok {
		itr := t.Iterator()
		otherItr := otherTuple.Iterator()

		if itr.Len() != otherItr.Len() {
			return false
		}

		for itr.HasMore() {
			_, val := itr.Next()
			_, otherVal := otherItr.Next()

			if !val.Equals(otherVal) {
				return false
			}
		}

		return true
	}

	return false
}*/

func (t Tuple) Less(other LesserValuable) bool {
	if otherTuple, ok := other.(Tuple); ok {
		itr := t.Iterator()
		otherItr := otherTuple.Iterator()
		for itr.HasMore() {
			if !otherItr.HasMore() {
				// equal up til the end of other. other is shorter, therefore it is less
				return false
			}

			_, currVal := itr.Next()
			_, currOthVal := otherItr.Next()

			if !currVal.Equals(currOthVal) {
				return currVal.Less(currOthVal)
			}
		}

		return itr.Len() < otherItr.Len()
	}

	return TupleKind < other.Kind()
}
