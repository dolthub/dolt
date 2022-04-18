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
	"errors"
	"fmt"
	"io"
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
	return emptyTuples[nbf]
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

func (itr *TupleIterator) NextUint64() (pos uint64, val uint64, err error) {
	if itr.pos < itr.count {
		k := itr.dec.ReadKind()

		if k != UintKind {
			return 0, 0, errors.New("NextUint64 called when the next value is not a Uint64")
		}

		valPos := itr.pos
		val := itr.dec.ReadUint()
		itr.pos++

		return valPos, val, nil
	}

	return itr.count, 0, io.EOF
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
	k := dec.PeekKind()

	if k == NullKind {
		dec.skipKind()
		return EmptyTuple(nbf), nil
	}

	if k != TupleKind {
		return Tuple{}, errors.New("current value is not a tuple")
	}

	err := skipTuple(nbf, dec)

	if err != nil {
		return Tuple{}, err
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

// TupleFactory provides a more memory efficient mechanism for creating many tuples
type TupleFactory struct {
	nbf            *NomsBinFormat
	biggestTuple   int
	approxCapacity int

	pos    int
	buffer []byte
}

// NewTupleFactory creates a new tuple factory. The approxCapacity argument is used to calculate how large the buffer allocations
// should be.  The factory keeps track of the largest tuple it has created, and when allocating it creates a buffer large enough
// to store <approxCapacity> tuples of that size.
func NewTupleFactory(approxCapacity int) *TupleFactory {
	blockSize := initialBufferSize * approxCapacity
	return &TupleFactory{
		buffer:         make([]byte, blockSize),
		biggestTuple:   initialBufferSize,
		approxCapacity: approxCapacity,
	}
}

// Reset is called when a TupleFactory is reused as you might want when pooling these.  Reset does
// not reset the buffer as the memory may be in use by tuples that have not been collected and reference the same
// memory.  It also does not reset biggestTuple.  It's ok for biggestTuple to grow as time goes on.
func (tf *TupleFactory) Reset(nbf *NomsBinFormat) {
	tf.nbf = nbf
}

func (tf *TupleFactory) newBuffer() {
	blockSize := tf.biggestTuple * tf.approxCapacity
	tf.buffer = make([]byte, blockSize)
	tf.pos = 0
}

// Create creates a new Tuple using the TupleFactory
func (tf *TupleFactory) Create(values ...Value) (Tuple, error) {
	remaining := len(tf.buffer) - tf.pos
	// somewhat wasteful, but it's costly if there isn't enough room to store a tuple in the tf's buffer so make it a rare case
	if remaining < tf.biggestTuple {
		tf.newBuffer()
		remaining = len(tf.buffer)
	}

	w := binaryNomsWriter{buff: tf.buffer[tf.pos:], offset: 0}
	t, err := newTuple(tf.nbf, w, values...)

	if err != nil {
		return Tuple{}, err
	}

	n := len(t.buff)

	// if n < bytes remaining then we move pos by the number of bytes read.  If not then a new allocation was used, and we don't move tf.pos
	if n < remaining {
		tf.pos += n
	}

	if n > tf.biggestTuple {
		tf.biggestTuple = n
	}

	return t, err
}

func NewTuple(nbf *NomsBinFormat, values ...Value) (Tuple, error) {
	w := newBinaryNomsWriter()
	return newTuple(nbf, w, values...)
}

func newTuple(nbf *NomsBinFormat, w binaryNomsWriter, values ...Value) (Tuple, error) {
	var vrw ValueReadWriter
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

// CopyOf creates a copy of a tuple.  This is necessary in cases where keeping a reference to the original tuple is
// preventing larger objects from being collected.
func (t Tuple) CopyOf(vrw ValueReadWriter) Tuple {
	buff := make([]byte, len(t.buff))
	offsets := make([]uint32, len(t.offsets))

	copy(buff, t.buff)
	copy(offsets, t.offsets)

	return Tuple{
		valueImpl{
			buff:    buff,
			offsets: offsets,
			vrw:     vrw,
			nbf:     t.nbf,
		},
	}
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

var tupleType = newType(CompoundDesc{UnionKind, nil})

func (t Tuple) typeOf() (*Type, error) {
	return tupleType, nil
}

func (t Tuple) decoderSkipToFields() (valueDecoder, uint64) {
	dec := t.decoder()
	dec.skipKind()
	count := dec.readCount()
	return dec, count
}

func (t Tuple) Size() int {
	return len(t.buff)
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

// AsSlice returns all of the values of this Tuple as a slice.
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

// AsSubslice returns the first n values of this Tuple as a slice.
func (t Tuple) AsSubslice(n uint64) (TupleValueSlice, error) {
	dec, count := t.decoderSkipToFields()
	if n < count {
		count = n
	}

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

func (t Tuple) Append(vals ...Value) (Tuple, error) {
	dec := t.decoder()
	dec.skipKind()
	prolog := dec.buff[:dec.offset]
	count := dec.readCount()
	fieldsOffset := dec.offset

	w := binaryNomsWriter{make([]byte, len(t.buff)), 0}
	w.writeRaw(prolog)
	w.writeCount(count + uint64(len(vals)))
	w.writeRaw(dec.buff[fieldsOffset:])
	for _, val := range vals {
		err := val.writeTo(&w, t.format())
		if err != nil {
			return EmptyTuple(t.nbf), err
		}
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

func (t Tuple) TupleCompare(nbf *NomsBinFormat, otherTuple Tuple) (int, error) {
	itrs := tupItrPairPool.Get().(*tupleItrPair)
	defer tupItrPairPool.Put(itrs)

	itr := itrs.thisItr
	err := itr.InitForTuple(t)

	if err != nil {
		return 0, err
	}

	otherItr := itrs.otherItr
	err = otherItr.InitForTuple(otherTuple)

	if err != nil {
		return 0, err
	}

	smallerCount := itr.count
	if otherItr.count < smallerCount {
		smallerCount = otherItr.count
	}

	dec := itr.dec
	otherDec := otherItr.dec
	for i := uint64(0); i < smallerCount; i++ {
		kind := dec.ReadKind()
		otherKind := otherDec.ReadKind()

		if kind != otherKind {
			return int(kind) - int(otherKind), nil
		}

		var res int
		switch kind {
		case NullKind:
			continue

		case BoolKind:
			res = int(dec.buff[dec.offset]) - int(otherDec.buff[otherDec.offset])
			dec.offset += 1
			otherDec.offset += 1

		case StringKind:
			size, otherSize := uint32(dec.readCount()), uint32(otherDec.readCount())
			start, otherStart := dec.offset, otherDec.offset
			dec.offset += size
			otherDec.offset += otherSize
			res = bytes.Compare(dec.buff[start:dec.offset], otherDec.buff[otherStart:otherDec.offset])

		case InlineBlobKind:
			size, otherSize := uint32(dec.readUint16()), uint32(otherDec.readUint16())
			start, otherStart := dec.offset, otherDec.offset
			dec.offset += size
			otherDec.offset += otherSize
			res = bytes.Compare(dec.buff[start:dec.offset], otherDec.buff[otherStart:otherDec.offset])

		case UUIDKind:
			start, otherStart := dec.offset, otherDec.offset
			dec.offset += uuidNumBytes
			otherDec.offset += uuidNumBytes
			res = bytes.Compare(dec.buff[start:dec.offset], otherDec.buff[otherStart:otherDec.offset])

		case IntKind:
			n := dec.ReadInt()
			otherN := otherDec.ReadInt()

			if n == otherN {
				continue
			} else {
				if n < otherN {
					return -1, nil
				}

				return 1, nil
			}

		case UintKind:
			n := dec.ReadUint()
			otherN := otherDec.ReadUint()

			if n == otherN {
				continue
			} else {
				if n < otherN {
					return -1, nil
				}

				return 1, nil
			}

		case DecimalKind:
			d, err := dec.ReadDecimal()

			if err != nil {
				return 0, err
			}

			otherD, err := otherDec.ReadDecimal()

			if err != nil {
				return 0, err
			}

			res = d.Cmp(otherD)

		case FloatKind:
			f := dec.ReadFloat(nbf)
			otherF := otherDec.ReadFloat(nbf)
			res = int(f - otherF)

			if f == otherF {
				continue
			} else {
				if f < otherF {
					return -1, nil
				}

				return 1, nil
			}

		case TimestampKind:
			tm, err := dec.ReadTimestamp()

			if err != nil {
				return 0, err
			}

			otherTm, err := otherDec.ReadTimestamp()

			if err != nil {
				return 0, err
			}

			if tm.Equal(otherTm) {
				continue
			} else {
				if tm.Before(otherTm) {
					return -1, nil
				}

				return 1, nil
			}

		case BlobKind:
			// readValue expects the Kind to still be there, so we put it back by decrementing the offset
			dec.offset--
			otherDec.offset--
			blob, err := dec.ReadBlob()
			if err != nil {
				return 0, err
			}
			otherBlob, err := otherDec.ReadBlob()
			if err != nil {
				return 0, err
			}
			res, err = blob.Compare(nbf, otherBlob)
			if err != nil {
				return 0, err
			}

		default:
			v, err := dec.readValue(nbf)

			if err != nil {
				return 0, err
			}

			otherV, err := otherDec.readValue(nbf)

			if err != nil {
				return 0, err
			}

			if v.Equals(otherV) {
				continue
			} else {
				isLess, err := v.Less(nbf, otherV)
				if err != nil {
					return 0, err
				} else if isLess {
					return -1, nil
				}

				return 1, nil
			}
		}

		if res != 0 {
			return res, nil
		}
	}

	return int(itr.Len()) - int(otherItr.Len()), nil
}

func (t Tuple) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	otherTuple, ok := other.(Tuple)
	if !ok {
		return TupleKind < other.Kind(), nil
	}

	res, err := t.TupleCompare(nbf, otherTuple)
	if err != nil {
		return false, err
	}

	return res < 0, err
}

func (t Tuple) Compare(nbf *NomsBinFormat, other LesserValuable) (int, error) {
	otherTuple, ok := other.(Tuple)
	if !ok {
		return int(TupleKind) - int(other.Kind()), nil
	}

	res, err := t.TupleCompare(nbf, otherTuple)
	if err != nil {
		return 0, err
	}

	return res, err
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
