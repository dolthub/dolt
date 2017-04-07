// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

type blobLeafSequence struct {
	leafSequence
	data []byte
}

func newBlobLeafSequence(vr ValueReader, data []byte) sequence {
	return blobLeafSequence{leafSequence{vr, len(data), BlobKind}, data}
}

// sequence interface

func (bl blobLeafSequence) getCompareFn(other sequence) compareFn {
	otherbl := other.(blobLeafSequence)
	return func(idx, otherIdx int) bool {
		return bl.data[idx] == otherbl.data[otherIdx]
	}
}

func (bl blobLeafSequence) getItem(idx int) sequenceItem {
	return bl.data[idx]
}

func (bl blobLeafSequence) WalkRefs(cb RefCallback) {
}

func (bl blobLeafSequence) typeOf() *Type {
	return BlobType
}
