// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

type blobLeafSequence struct {
	leafSequence
}

func newBlobLeafSequence(vrw ValueReadWriter, data []byte) sequence {
	d.PanicIfTrue(vrw == nil)
	offsets := make([]uint32, sequencePartValues+1)
	w := newBinaryNomsWriter()
	offsets[sequencePartKind] = w.offset
	BlobKind.writeTo(&w)
	offsets[sequencePartLevel] = w.offset
	w.writeCount(0) // level
	offsets[sequencePartCount] = w.offset
	count := uint64(len(data))
	w.writeCount(count)
	offsets[sequencePartValues] = w.offset
	w.writeBytes(data)
	return blobLeafSequence{newLeafSequence(vrw, w.data(), offsets, count)}
}

func (bl blobLeafSequence) writeTo(w nomsWriter) {
	w.writeRaw(bl.buff)
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
	return func(idx, otherIdx int) bool {
		return bl.buff[offsetStart+idx] == obl.buff[otherOffsetStart+otherIdx]
	}
}

func (bl blobLeafSequence) getItem(idx int) sequenceItem {
	offset := bl.offsets[sequencePartValues] - bl.offsets[sequencePartKind] + uint32(idx)
	return bl.buff[offset]
}

func (bl blobLeafSequence) typeOf() *Type {
	return BlobType
}
