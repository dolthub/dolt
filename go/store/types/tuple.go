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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/store/d"
)

var _ LesserValuable = TupleValueSlice(nil)

type TupleValueSlice []Value

func (tvs TupleValueSlice) Kind() NomsKind {
	return TupleKind
}

func (tvs TupleValueSlice) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	switch typedOther := other.(type) {
	case Tuple:
		val, err := NewTuple(nbf, tvs...)

		if err != nil {
			return false, err
		}

		return typedOther.Less(nbf, val)

	case TupleValueSlice:
		myLen := len(tvs)
		otherLen := len(typedOther)
		largerLen := myLen
		if otherLen > largerLen {
			largerLen = otherLen
		}

		var val Value
		var otherVal Value
		for i := 0; i < largerLen; i++ {
			if i < myLen {
				val = tvs[i]
			}

			if i < otherLen {
				otherVal = typedOther[i]
			}

			if val == nil {
				return true, nil
			} else if otherVal == nil {
				return false, nil
			}

			if !val.Equals(otherVal) {
				return val.Less(nbf, otherVal)
			}
		}

		return false, nil
	default:
		return TupleKind < other.Kind(), nil
	}
}

func (tvs TupleValueSlice) Value(ctx context.Context) (Value, error) {
	panic("not implemented")
}

func EmptyTuple(nbf *NomsBinFormat) Tuple {
	t, err := NewTuple(nbf)
	d.PanicIfError(err)

	return t
}

func newTupleIterator() interface{} {
	return &TupleIterator{}
}

type tupleItrPair struct {
	thisItr  *TupleIterator
	otherItr *TupleIterator
}

func newTupleIteratorPair() interface{} {
	return &tupleItrPair{&TupleIterator{}, &TupleIterator{}}
}

var TupleItrPool = &sync.Pool{New: newTupleIterator}
var tupItrPairPool = &sync.Pool{New: newTupleIteratorPair}

type TupleIterator struct {
	dec   *valueDecoder
	count uint64
	pos   uint64
	nbf   *NomsBinFormat
}

func (itr *TupleIterator) Next() (uint64, Value, error) {
	if itr.pos < itr.count {
		valPos := itr.pos
		val, err := itr.dec.readValue(itr.nbf)

		if err != nil {
			return 0, nil, err
		}

		itr.pos++
		return valPos, val, nil
	}

	return itr.count, nil, nil
}

func (itr *TupleIterator) CodecReader() (CodecReader, uint64) {
	return itr.dec, itr.count - itr.pos
}

func (itr *TupleIterator) Skip() error {
	if itr.pos < itr.count {
		err := itr.dec.SkipValue(itr.nbf)

		if err != nil {
			return err
		}

		itr.pos++
	}

	return nil
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

func (itr *TupleIterator) InitForTuple(t Tuple) error {
	return itr.InitForTupleAt(t, 0)
}

func (itr *TupleIterator) InitForTupleAt(t Tuple, pos uint64) error {
	if itr.dec == nil {
		dec := t.decoder()
		itr.dec = &dec
	} else {
		itr.dec.buff = t.buff
		itr.dec.offset = 0
		itr.dec.vrw = t.vrw
	}

	itr.dec.skipKind()
	count := itr.dec.readCount()

	for i := uint64(0); i < pos; i++ {
		err := itr.dec.SkipValue(t.format())

		if err != nil {
			return err
		}
	}

	itr.count = count
	itr.pos = pos
	itr.nbf = t.format()
	return nil
}

type Tuple struct {
	valueImpl
}

// readTuple reads the data provided by a decoder and moves the decoder forward.
func readTuple(nbf *NomsBinFormat, dec *valueDecoder) (Tuple, error) {
	start := dec.pos()
	err := skipTuple(nbf, dec)

	if err != nil {
		return EmptyTuple(nbf), err
	}
	end := dec.pos()
	return Tuple{valueImpl{dec.vrw, nbf, dec.byteSlice(start, end), nil}}, nil
}

func skipTuple(nbf *NomsBinFormat, dec *valueDecoder) error {
	dec.skipKind()
	count := dec.readCount()
	for i := uint64(0); i < count; i++ {
		err := dec.SkipValue(nbf)

		if err != nil {
			return err
		}
	}
	return nil
}

func walkTuple(nbf *NomsBinFormat, r *refWalker, cb RefCallback) error {
	r.skipKind()
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		err := r.walkValue(nbf, cb)

		if err != nil {
			return err
		}
	}
	return nil
}

func NewTuple(nbf *NomsBinFormat, values ...Value) (Tuple, error) {
	var vrw ValueReadWriter
	w := newBinaryNomsWriter()
	err := TupleKind.writeTo(&w, nbf)

	if err != nil {
		return EmptyTuple(nbf), err
	}

	numVals := len(values)
	w.writeCount(uint64(numVals))
	for i := 0; i < numVals; i++ {
		if vrw == nil {
			vrw = values[i].(valueReadWriter).valueReadWriter()
		}
		err := values[i].writeTo(&w, nbf)

		if err != nil {
			return EmptyTuple(nbf), err
		}
	}

	return Tuple{valueImpl{vrw, nbf, w.data(), nil}}, nil
}

func (t Tuple) Empty() bool {
	return t.Len() == 0
}

func (t Tuple) Format() *NomsBinFormat {
	return t.format()
}

// Value interface
func (t Tuple) Value(ctx context.Context) (Value, error) {
	return t, nil
}

func (t Tuple) WalkValues(ctx context.Context, cb ValueCallback) error {
	dec, count := t.decoderSkipToFields()
	for i := uint64(0); i < count; i++ {
		v, err := dec.readValue(t.format())

		if err != nil {
			return err
		}

		err = cb(v)

		if err != nil {
			return err
		}
	}

	return nil
}

// PrefixEquals returns whether the given Tuple and calling Tuple have equivalent values up to the given count. Useful
// for testing Tuple equality for partial keys. If the Tuples are not of the same length, and one Tuple's length is less
// than the given count, then this returns false. If the Tuples are of the same length and they're both less than the
// given count, then this function is equivalent to Equals.
func (t Tuple) PrefixEquals(ctx context.Context, other Tuple, prefixCount uint64) (bool, error) {
	tDec, tCount := t.decoderSkipToFields()
	otherDec, otherCount := other.decoderSkipToFields()
	if tCount == otherCount && tCount < prefixCount {
		return t.Equals(other), nil
	} else if tCount != otherCount && (tCount < prefixCount || otherCount < prefixCount) {
		return false, nil
	}
	for i := uint64(0); i < prefixCount; i++ {
		val, err := tDec.readValue(t.format())
		if err != nil {
			return false, err
		}
		otherVal, err := otherDec.readValue(t.format())
		if err != nil {
			return false, err
		}
		if !val.Equals(otherVal) {
			return false, nil
		}
	}
	return true, nil
}

func (t Tuple) typeOf() (*Type, error) {
	dec, count := t.decoderSkipToFields()
	ts := make(typeSlice, 0, count)
	var lastType *Type
	for i := uint64(0); i < count; i++ {
		if lastType != nil {
			offset := dec.offset
			is, err := dec.isValueSameTypeForSure(t.format(), lastType)

			if err != nil {
				return nil, err
			}

			if is {
				continue
			}
			dec.offset = offset
		}

		var err error
		lastType, err = dec.readTypeOfValue(t.format())

		if err != nil {
			return nil, err
		}

		if lastType.Kind() == UnknownKind {
			// if any of the elements are unknown, return unknown
			return nil, ErrUnknownType
		}

		ts = append(ts, lastType)
	}

	ut, err := makeUnionType(ts...)

	if err != nil {
		return nil, err
	}

	return makeCompoundType(TupleKind, ut)
}

func (t Tuple) decoderSkipToFields() (valueDecoder, uint64) {
	dec := t.decoder()
	dec.skipKind()
	count := dec.readCount()
	return dec, count
}

// Len is the number of fields in the struct.
func (t Tuple) Len() uint64 {
	if len(t.buff) == 0 {
		return 0
	}
	_, count := t.decoderSkipToFields()
	return count
}

func (t Tuple) isPrimitive() bool {
	return false
}

func (t Tuple) Iterator() (*TupleIterator, error) {
	return t.IteratorAt(0)
}

func (t Tuple) IteratorAt(pos uint64) (*TupleIterator, error) {
	itr := &TupleIterator{}
	err := itr.InitForTupleAt(t, pos)

	if err != nil {
		return nil, err
	}

	return itr, nil
}

func (t Tuple) AsSlice() (TupleValueSlice, error) {
	dec, count := t.decoderSkipToFields()

	sl := make(TupleValueSlice, count)
	for pos := uint64(0); pos < count; pos++ {
		val, err := dec.readValue(t.nbf)

		if err != nil {
			return nil, err
		}

		sl[pos] = val
	}

	return sl, nil
}

// IterFields iterates over the fields, calling cb for every field in the tuple until cb returns false
func (t Tuple) IterFields(cb func(index uint64, value Value) (stop bool, err error)) error {
	itr := TupleItrPool.Get().(*TupleIterator)
	defer TupleItrPool.Put(itr)

	err := itr.InitForTuple(t)

	if err != nil {
		return err
	}

	for itr.HasMore() {
		i, curr, err := itr.Next()

		if err != nil {
			return err
		}

		stop, err := cb(i, curr)

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

// Get returns the value of a field in the tuple. If the tuple does not a have a field at the index then this panics
func (t Tuple) Get(n uint64) (Value, error) {
	dec, count := t.decoderSkipToFields()

	if n >= count {
		d.Chk.Fail(fmt.Sprintf(`tuple index "%d" out of range`, n))
	}

	for i := uint64(0); i < n; i++ {
		err := dec.SkipValue(t.format())

		if err != nil {
			return nil, err
		}
	}

	return dec.readValue(t.format())
}

// Set returns a new tuple where the field at index n is set to value. Attempting to use Set on an index that is outside
// of the bounds will cause a panic.  Use Append to add additional values, not Set.
func (t Tuple) Set(n uint64, v Value) (Tuple, error) {
	prolog, head, tail, count, found, err := t.splitFieldsAt(n)

	if err != nil {
		return EmptyTuple(t.nbf), err
	}

	if !found {
		d.Panic("Cannot set tuple value at index %d as it is outside the range [0,%d]", n, count-1)
	}

	w := binaryNomsWriter{make([]byte, len(t.buff)), 0}
	w.writeRaw(prolog)

	w.writeCount(count)
	w.writeRaw(head)
	err = v.writeTo(&w, t.format())

	if err != nil {
		return EmptyTuple(t.nbf), err
	}
	w.writeRaw(tail)

	return Tuple{valueImpl{t.vrw, t.format(), w.data(), nil}}, nil
}

func (t Tuple) Append(v Value) (Tuple, error) {
	dec := t.decoder()
	dec.skipKind()
	prolog := dec.buff[:dec.offset]
	count := dec.readCount()
	fieldsOffset := dec.offset

	w := binaryNomsWriter{make([]byte, len(t.buff)), 0}
	w.writeRaw(prolog)
	w.writeCount(count + 1)
	w.writeRaw(dec.buff[fieldsOffset:])
	err := v.writeTo(&w, t.format())

	if err != nil {
		return EmptyTuple(t.nbf), err
	}

	return Tuple{valueImpl{t.vrw, t.format(), w.data(), nil}}, nil
}

// splitFieldsAt splits the buffer into two parts. The fields coming before the field we are looking for
// and the fields coming after it.
func (t Tuple) splitFieldsAt(n uint64) (prolog, head, tail []byte, count uint64, found bool, err error) {
	dec := t.decoder()
	dec.skipKind()
	prolog = dec.buff[:dec.offset]
	count = dec.readCount()

	if n >= count {
		return nil, nil, nil, count, false, nil
	}

	found = true
	fieldsOffset := dec.offset

	for i := uint64(0); i < n; i++ {
		err := dec.SkipValue(t.format())

		if err != nil {
			return nil, nil, nil, 0, false, err
		}
	}

	head = dec.buff[fieldsOffset:dec.offset]

	if n != count-1 {
		err := dec.SkipValue(t.format())

		if err != nil {
			return nil, nil, nil, 0, false, err
		}

		tail = dec.buff[dec.offset:len(dec.buff)]
	}

	return
}

func (t Tuple) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if otherTuple, ok := other.(Tuple); ok {
		itrs := tupItrPairPool.Get().(*tupleItrPair)
		defer tupItrPairPool.Put(itrs)

		itr := itrs.thisItr
		err := itr.InitForTuple(t)

		if err != nil {
			return false, err
		}

		otherItr := itrs.otherItr
		err = otherItr.InitForTuple(otherTuple)

		if err != nil {
			return false, err
		}

		for itr.HasMore() {
			if !otherItr.HasMore() {
				// equal up til the end of other. other is shorter, therefore it is less
				return false, nil
			}

			_, currVal, err := itr.Next()

			if err != nil {
				return false, err
			}

			_, currOthVal, err := otherItr.Next()

			if err != nil {
				return false, err
			}

			if !currVal.Equals(currOthVal) {
				return currVal.Less(nbf, currOthVal)
			}
		}

		return itr.Len() < otherItr.Len(), nil
	}

	return TupleKind < other.Kind(), nil
}

func (t Tuple) StartsWith(otherTuple Tuple) bool {
	tplDec, _ := t.decoderSkipToFields()
	otherDec, _ := otherTuple.decoderSkipToFields()
	return bytes.HasPrefix(tplDec.buff[tplDec.offset:], otherDec.buff[otherDec.offset:])
}

func (t Tuple) Contains(v Value) (bool, error) {
	itr := TupleItrPool.Get().(*TupleIterator)
	defer TupleItrPool.Put(itr)

	err := itr.InitForTuple(t)
	if err != nil {
		return false, err
	}

	for itr.HasMore() {
		_, tupleVal, err := itr.Next()
		if err != nil {
			return false, err
		}
		if tupleVal.Equals(v) {
			return true, nil
		}
	}
	return false, nil
}

func (t Tuple) readFrom(nbf *NomsBinFormat, b *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (t Tuple) skip(nbf *NomsBinFormat, b *binaryNomsReader) {
	panic("unreachable")
}

func (t Tuple) String() string {
	b := strings.Builder{}

	iter := TupleItrPool.Get().(*TupleIterator)
	defer TupleItrPool.Put(iter)

	err := iter.InitForTuple(t)
	if err != nil {
		b.WriteString(err.Error())
		return b.String()
	}

	b.WriteString("Tuple(")

	seenOne := false
	for {
		_, v, err := iter.Next()
		if v == nil {
			break
		}
		if err != nil {
			b.WriteString(err.Error())
			return b.String()
		}

		if seenOne {
			b.WriteString(", ")
		}
		seenOne = true

		b.WriteString(v.HumanReadableString())
	}
	b.WriteString(")")
	return b.String()
}

func (t Tuple) HumanReadableString() string {
	return t.String()
}
