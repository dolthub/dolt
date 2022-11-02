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

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

type sequenceItem interface{}

type compareFn func(x int, y int) (bool, error)

type sequence interface {
	asValueImpl() valueImpl
	cumulativeNumberOfLeaves(idx int) (uint64, error)
	Empty() bool
	Equals(other Value) bool
	format() *NomsBinFormat
	getChildSequence(ctx context.Context, idx int) (sequence, error)
	getCompareFn(other sequence) compareFn
	getCompositeChildSequence(ctx context.Context, start uint64, length uint64) (sequence, error)
	getItem(idx int) (sequenceItem, error)
	Hash(*NomsBinFormat) (hash.Hash, error)
	isLeaf() bool
	Kind() NomsKind
	Len() uint64
	Less(nbf *NomsBinFormat, other LesserValuable) (bool, error)
	Compare(nbf *NomsBinFormat, other LesserValuable) (int, error)
	numLeaves() uint64
	seqLen() int
	treeLevel() uint64
	typeOf() (*Type, error)
	valueReadWriter() ValueReadWriter
	valuesSlice(from, to uint64) ([]Value, error)
	kvTuples(from, to uint64, dest []Tuple) ([]Tuple, error)
	walkRefs(nbf *NomsBinFormat, cb RefCallback) error
	writeTo(nomsWriter, *NomsBinFormat) error
}

const (
	sequencePartKind   = 0
	sequencePartLevel  = 1
	sequencePartCount  = 2
	sequencePartValues = 3
)

type sequenceImpl struct {
	valueImpl
	len uint64
}

func newSequenceImpl(nbf *NomsBinFormat, vrw ValueReadWriter, buff []byte, offsets []uint32, len uint64) sequenceImpl {
	if vrw != nil {
		d.PanicIfFalse(nbf == vrw.Format())
	}
	return sequenceImpl{valueImpl{vrw, nbf, buff, offsets}, len}
}

func (seq sequenceImpl) decoderSkipToValues() (valueDecoder, uint64) {
	dec := seq.decoderAtPart(sequencePartCount)
	count := dec.readCount()
	return dec, count
}

func (seq sequenceImpl) decoderAtPart(part uint32) valueDecoder {
	offset := seq.offsets[part] - seq.offsets[sequencePartKind]
	return newValueDecoder(seq.buff[offset:], seq.vrw)
}

func (seq sequenceImpl) Empty() bool {
	return seq.Len() == 0
}

func (seq sequenceImpl) Len() uint64 {
	return seq.len
}

func (seq sequenceImpl) seqLen() int {
	_, count := seq.decoderSkipToValues()
	return int(count)
}

func (seq sequenceImpl) getItemOffset(idx int) int {
	// kind, level, count, elements...
	// 0     1      2      3          n+1
	d.PanicIfTrue(idx+sequencePartValues+1 > len(seq.offsets))
	return int(seq.offsets[idx+sequencePartValues] - seq.offsets[sequencePartKind])
}

func (seq sequenceImpl) decoderSkipToIndex(idx int) valueDecoder {
	offset := seq.getItemOffset(idx)
	return seq.decoderAtOffset(offset)
}
