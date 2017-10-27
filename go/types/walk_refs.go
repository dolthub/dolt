// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
)

// WalkRefs calls cb() on each Ref that can be decoded from |c|. The results
// are precisely equal to DecodeValue(c).WalkRefs(cb), but this should be much
// faster.
func WalkRefs(c chunks.Chunk, cb RefCallback) {
	walkRefs(c.Data(), cb)
}

func walkRefs(data []byte, cb RefCallback) {
	rw := newRefWalker(data)
	rw.walkValue(cb)
}

type refWalker struct {
	typedBinaryNomsReader
}

func newRefWalker(buff []byte) refWalker {
	nr := binaryNomsReader{buff, 0}
	return refWalker{typedBinaryNomsReader{nr, false}}
}

func (r *refWalker) walkRef(cb RefCallback) {
	cb(readRef(&(r.typedBinaryNomsReader)))
}

func (r *refWalker) walkBlobLeafSequence() {
	size := r.readCount()
	r.offset += uint32(size)
}

func (r *refWalker) walkValueSequence(cb RefCallback) {
	count := int(r.readCount())
	for i := 0; i < count; i++ {
		r.walkValue(cb)
	}
}

func (r *refWalker) walkList(cb RefCallback) {
	r.walkListOrSet(ListKind, cb)
}

func (r *refWalker) walkSet(cb RefCallback) {
	r.walkListOrSet(SetKind, cb)
}

func (r *refWalker) walkListOrSet(kind NomsKind, cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(kind, level, cb)
	} else {
		r.walkValueSequence(cb)
	}
}

func (r *refWalker) walkMap(cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(MapKind, level, cb)
	} else {
		r.walkMapLeafSequence(cb)
	}
}

func (r *refWalker) walkBlob(cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(BlobKind, level, cb)
	} else {
		r.walkBlobLeafSequence()
	}
}

func (r *refWalker) walkMapLeafSequence(cb RefCallback) {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.walkValue(cb) // k
		r.walkValue(cb) // v
	}
}

func (r *refWalker) walkMetaSequence(k NomsKind, level uint64, cb RefCallback) {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.walkRef(cb) // ref to child sequence
		r.skipOrderedKey()
		r.skipCount() // numLeaves
	}
}

func (r *refWalker) skipOrderedKey() {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		r.skipHash()
	default:
		r.walkValue(func(r Ref) {}) // max Value in subtree reachable from here
	}
}

func (r *refWalker) walkValue(cb RefCallback) {
	k := r.peekKind()
	switch k {
	case BlobKind:
		r.walkBlob(cb)
	case BoolKind:
		r.skipKind()
		r.skipBool()
	case NumberKind:
		r.skipKind()
		r.skipNumber()
	case StringKind:
		r.skipKind()
		r.skipString()
	case ListKind:
		r.walkList(cb)
	case MapKind:
		r.walkMap(cb)
	case RefKind:
		r.walkRef(cb)
	case SetKind:
		r.walkSet(cb)
	case StructKind:
		r.walkStruct(cb)
	case TypeKind:
		r.skipKind()
		r.skipType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		panic("not reachable")
	}
}

func (r *refWalker) walkStruct(cb RefCallback) {
	walkStruct(r, cb)
}
