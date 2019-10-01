// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/store/d"
)

func EmptyTuple(nbf *NomsBinFormat) Tuple {
	t, err := NewTuple(nbf)
	d.PanicIfError(err)

	return t
}

type TupleIterator struct {
	dec   valueDecoder
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
		err := dec.skipValue(nbf)

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
	_, count := t.decoderSkipToFields()
	return count
}

func (t Tuple) Iterator() (*TupleIterator, error) {
	return t.IteratorAt(0)
}

func (t Tuple) IteratorAt(pos uint64) (*TupleIterator, error) {
	dec, count := t.decoderSkipToFields()

	for i := uint64(0); i < pos; i++ {
		err := dec.skipValue(t.format())

		if err != nil {
			return nil, err
		}
	}

	return &TupleIterator{dec, count, pos, t.format()}, nil
}

// IterFields iterates over the fields, calling cb for every field in the tuple until cb returns false
func (t Tuple) IterFields(cb func(index uint64, value Value) (stop bool, err error)) error {
	itr, err := t.Iterator()

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
		err := dec.skipValue(t.format())

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
		err := dec.skipValue(t.format())

		if err != nil {
			return nil, nil, nil, 0, false, err
		}
	}

	head = dec.buff[fieldsOffset:dec.offset]

	if n != count-1 {
		err := dec.skipValue(t.format())

		if err != nil {
			return nil, nil, nil, 0, false, err
		}

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

func (t Tuple) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if otherTuple, ok := other.(Tuple); ok {
		itr, err := t.Iterator()

		if err != nil {
			return false, err
		}

		otherItr, err := otherTuple.Iterator()

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

// CountDifferencesBetweenTupleFields returns the number of fields that are different between two
// tuples and does not panic if tuples are different lengths.
func (t Tuple) CountDifferencesBetweenTupleFields(other Tuple) (uint64, error) {
	changed := 0

	err := t.IterFields(func(index uint64, val Value) (stop bool, err error) {
		dec, count := other.decoderSkipToFields()

		// Prevents comparing column tags
		if index%2 == 1 {
			if index >= count {
				changed++
				return false, nil
			} else {
				for i := uint64(0); i < index; i++ {
					err := dec.skipValue(other.format())

					if err != nil {
						return true, err
					}
				}
				otherVal, err := dec.readValue(other.format())
				if err != nil {
					return true, err
				}
				if !otherVal.Equals(val) {
					changed++
				}
			}
		}

		return false, nil
	})

	if err != nil {
		return 0, err
	}
	return uint64(changed), nil
}
