// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

// WalkRefs calls cb() on each Ref that can be decoded from |c|. The results
// are precisely equal to DecodeValue(c).WalkRefs(cb), but this should be much
// faster.
func WalkRefs(c chunks.Chunk, f *Format, cb RefCallback) {
	walkRefs(c.Data(), f, cb)
}

func walkRefs(data []byte, f *Format, cb RefCallback) {
	rw := newRefWalker(data)
	rw.walkValue(f, cb)
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

func (r *refWalker) walkValueSequence(f *Format, cb RefCallback) {
	count := int(r.readCount())
	for i := 0; i < count; i++ {
		r.walkValue(f, cb)
	}
}

func (r *refWalker) walkList(f *Format, cb RefCallback) {
	r.walkListOrSet(f, ListKind, cb)
}

func (r *refWalker) walkSet(f *Format, cb RefCallback) {
	r.walkListOrSet(f, SetKind, cb)
}

func (r *refWalker) walkListOrSet(f *Format, kind NomsKind, cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(f, kind, level, cb)
	} else {
		r.walkValueSequence(f, cb)
	}
}

func (r *refWalker) walkMap(f *Format, cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(f, MapKind, level, cb)
	} else {
		r.walkMapLeafSequence(f, cb)
	}
}

func (r *refWalker) walkBlob(f *Format, cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(f, BlobKind, level, cb)
	} else {
		r.walkBlobLeafSequence()
	}
}

func (r *refWalker) walkMapLeafSequence(f *Format, cb RefCallback) {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.walkValue(f, cb) // k
		r.walkValue(f, cb) // v
	}
}

func (r *refWalker) walkMetaSequence(f *Format, k NomsKind, level uint64, cb RefCallback) {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.walkRef(cb) // ref to child sequence
		r.skipOrderedKey(f)
		r.skipCount() // numLeaves
	}
}

func (r *refWalker) skipOrderedKey(f *Format) {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		r.skipHash()
	default:
		r.walkValue(f, func(r Ref) {}) // max Value in subtree reachable from here
	}
}

func (r *refWalker) walkValue(f *Format, cb RefCallback) {
	k := r.peekKind()
	switch k {
	case BlobKind:
		r.walkBlob(f, cb)
	case BoolKind:
		r.skipKind()
		r.skipBool()
	case FloatKind:
		r.skipKind()
		r.skipFloat(f)
	case IntKind:
		r.skipKind()
		r.skipInt()
	case UintKind:
		r.skipKind()
		r.skipUint()
	case UUIDKind:
		r.skipKind()
		r.skipUUID()
	case NullKind:
		r.skipKind()
	case StringKind:
		r.skipKind()
		r.skipString()
	case ListKind:
		r.walkList(f, cb)
	case MapKind:
		r.walkMap(f, cb)
	case RefKind:
		r.walkRef(cb)
	case SetKind:
		r.walkSet(f, cb)
	case StructKind:
		r.walkStruct(f, cb)
	case TupleKind:
		r.walkTuple(cb)
	case TypeKind:
		r.skipKind()
		r.skipType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		panic("not reachable")
	}
}

func (r *refWalker) walkStruct(f *Format, cb RefCallback) {
	walkStruct(f, r, cb)
}

func (r *refWalker) walkTuple(cb RefCallback) {
	walkTuple(r, cb)
}
