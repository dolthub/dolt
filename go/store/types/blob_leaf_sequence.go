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

import "github.com/dolthub/dolt/go/store/d"

type blobLeafSequence struct {
	leafSequence
}

func newBlobLeafSequence(nbf *NomsBinFormat, vrw ValueReadWriter, data []byte) (sequence, error) {
	d.PanicIfTrue(vrw == nil)
	offsets := make([]uint32, sequencePartValues+1)
	w := newBinaryNomsWriter()
	offsets[sequencePartKind] = w.offset
	err := BlobKind.writeTo(&w, vrw.Format())

	if err != nil {
		return nil, err
	}

	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(data))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	w.writeRaw(data)
	return blobLeafSequence{newLeafSequence(nbf, vrw, w.data(), offsets, count)}, nil
}

func (bl blobLeafSequence) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	w.writeRaw(bl.buff)
	return nil
}

// sequence interface

func (bl blobLeafSequence) data() []byte {
	offset := bl.offsets[sequencePartValues] - bl.offsets[sequencePartKind]
	return bl.buff[offset:]
}

func (bl blobLeafSequence) getCompareFn(other sequence) compareFn {
	offsetStart := int(bl.offsets[sequencePartValues] - bl.offsets[sequencePartKind])
	obl := other.(blobLeafSequence)
	otherOffsetStart := int(obl.offsets[sequencePartValues] - obl.offsets[sequencePartKind])
	return func(idx, otherIdx int) (bool, error) {
		return bl.buff[offsetStart+idx] == obl.buff[otherOffsetStart+otherIdx], nil
	}
}

func (bl blobLeafSequence) getItem(idx int) (sequenceItem, error) {
	offset := bl.offsets[sequencePartValues] - bl.offsets[sequencePartKind] + uint32(idx)
	return bl.buff[offset], nil
}

func (bl blobLeafSequence) typeOf() (*Type, error) {
	return PrimitiveTypeMap[BlobKind], nil
}
