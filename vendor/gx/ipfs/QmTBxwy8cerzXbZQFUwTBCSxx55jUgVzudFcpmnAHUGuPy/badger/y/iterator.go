/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"bytes"
	"container/heap"
	"encoding/binary"

	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
)

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
type ValueStruct struct {
	Value      []byte
	Meta       byte
	UserMeta   byte
	CASCounter uint64
}

// MakeValueStruct is the most convenient way for unit tests to make a ValueStruct.  (Also, the
// code will break if we add another field.)
func MakeValueStruct(value []byte, meta byte, userMeta byte, casCounter uint64) ValueStruct {
	return ValueStruct{Value: value, Meta: meta, UserMeta: userMeta, CASCounter: casCounter}
}

// EncodedSize is the size of the ValueStruct when encoded
func (v *ValueStruct) EncodedSize() int {
	return len(v.Value) + valueValueOffset
}

// ValueStructSerializedSize converts a value size to the full serialized size of value + metadata.
func ValueStructSerializedSize(size uint16) int {
	return int(size) + valueValueOffset
}

const (
	valueMetaOffset     = 0
	valueUserMetaOffset = valueMetaOffset + MetaSize
	valueCasOffset      = valueUserMetaOffset + UserMetaSize
	valueValueOffset    = valueCasOffset + CasSize
)

// DecodeEntireSlice uses the length of the slice to infer the length of the Value field.
func (v *ValueStruct) DecodeEntireSlice(b []byte) {
	v.Value = b[valueValueOffset:]
	v.Meta = b[valueMetaOffset]
	v.UserMeta = b[valueUserMetaOffset]
	v.CASCounter = binary.BigEndian.Uint64(b[valueCasOffset : valueCasOffset+CasSize])
}

// Encode expects a slice of length at least v.EncodedSize().
func (v *ValueStruct) Encode(b []byte) {
	b[valueMetaOffset] = v.Meta
	b[valueUserMetaOffset] = v.UserMeta
	binary.BigEndian.PutUint64(b[valueCasOffset:valueCasOffset+CasSize], v.CASCounter)
	copy(b[valueValueOffset:valueValueOffset+len(v.Value)], v.Value)
}

// Iterator is an interface for a basic iterator.
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ValueStruct
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}

type elem struct {
	itr      Iterator
	nice     int
	reversed bool
}

type elemHeap []*elem

func (eh elemHeap) Len() int            { return len(eh) }
func (eh elemHeap) Swap(i, j int)       { eh[i], eh[j] = eh[j], eh[i] }
func (eh *elemHeap) Push(x interface{}) { *eh = append(*eh, x.(*elem)) }
func (eh *elemHeap) Pop() interface{} {
	// Remove the last element, because Go has already swapped 0th elem <-> last.
	old := *eh
	n := len(old)
	x := old[n-1]
	*eh = old[0 : n-1]
	return x
}
func (eh elemHeap) Less(i, j int) bool {
	cmp := bytes.Compare(eh[i].itr.Key(), eh[j].itr.Key())
	if cmp < 0 {
		return !eh[i].reversed
	}
	if cmp > 0 {
		return eh[i].reversed
	}
	// The keys are equal. In this case, lower nice take precedence. This is important.
	return eh[i].nice < eh[j].nice
}

// MergeIterator merges multiple iterators.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	h        elemHeap
	curKey   []byte
	reversed bool

	all []Iterator
}

// NewMergeIterator returns a new MergeIterator from a list of Iterators.
func NewMergeIterator(iters []Iterator, reversed bool) *MergeIterator {
	m := &MergeIterator{all: iters, reversed: reversed}
	m.h = make(elemHeap, 0, len(iters))
	m.initHeap()
	return m
}

func (s *MergeIterator) storeKey(smallest Iterator) {
	if cap(s.curKey) < len(smallest.Key()) {
		s.curKey = make([]byte, 2*len(smallest.Key()))
	}
	s.curKey = s.curKey[:len(smallest.Key())]
	copy(s.curKey, smallest.Key())
}

// initHeap checks all iterators and initializes our heap and array of keys.
// Whenever we reverse direction, we need to run this.
func (s *MergeIterator) initHeap() {
	s.h = s.h[:0]
	for idx, itr := range s.all {
		if !itr.Valid() {
			continue
		}
		e := &elem{itr: itr, nice: idx, reversed: s.reversed}
		s.h = append(s.h, e)
	}
	heap.Init(&s.h)
	for len(s.h) > 0 {
		it := s.h[0].itr
		if it == nil || !it.Valid() {
			heap.Pop(&s.h)
			continue
		}
		s.storeKey(s.h[0].itr)
		break
	}
}

// Valid returns whether the MergeIterator is at a valid element.
func (s *MergeIterator) Valid() bool {
	if s == nil {
		return false
	}
	if len(s.h) == 0 {
		return false
	}
	return s.h[0].itr.Valid()
}

// Key returns the key associated with the current iterator
func (s *MergeIterator) Key() []byte {
	if len(s.h) == 0 {
		return nil
	}
	return s.h[0].itr.Key()
}

// Value returns the value associated with the iterator.
func (s *MergeIterator) Value() ValueStruct {
	if len(s.h) == 0 {
		return ValueStruct{}
	}
	return s.h[0].itr.Value()
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (s *MergeIterator) Next() {
	if len(s.h) == 0 {
		return
	}

	smallest := s.h[0].itr
	smallest.Next()

	for len(s.h) > 0 {
		smallest = s.h[0].itr
		if !smallest.Valid() {
			heap.Pop(&s.h)
			continue
		}

		heap.Fix(&s.h, 0)
		smallest = s.h[0].itr
		if smallest.Valid() {
			if !bytes.Equal(smallest.Key(), s.curKey) {
				break
			}
			smallest.Next()
		}
	}
	if !smallest.Valid() {
		return
	}
	s.storeKey(smallest)
}

// Rewind seeks to first element (or last element for reverse iterator).
func (s *MergeIterator) Rewind() {
	for _, itr := range s.all {
		itr.Rewind()
	}
	s.initHeap()
}

// Seek brings us to element with key >= given key.
func (s *MergeIterator) Seek(key []byte) {
	for _, itr := range s.all {
		itr.Seek(key)
	}
	s.initHeap()
}

// Close implements y.Iterator
func (s *MergeIterator) Close() error {
	for _, itr := range s.all {
		if err := itr.Close(); err != nil {
			return errors.Wrap(err, "MergeIterator")
		}
	}
	return nil
}
