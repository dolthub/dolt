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
func WalkRefs(c chunks.Chunk, nbf *NomsBinFormat, cb RefCallback) {
	walkRefs(c.Data(), nbf, cb)
}

func walkRefs(data []byte, nbf *NomsBinFormat, cb RefCallback) {
	rw := newRefWalker(data)
	rw.walkValue(nbf, cb)
}

type refWalker struct {
	typedBinaryNomsReader
}

func newRefWalker(buff []byte) refWalker {
	nr := binaryNomsReader{buff, 0}
	return refWalker{typedBinaryNomsReader{nr, false}}
}

func (r *refWalker) walkRef(nbf *NomsBinFormat, cb RefCallback) {
	cb(readRef(nbf, &(r.typedBinaryNomsReader)))
}

func (r *refWalker) walkBlobLeafSequence() {
	size := r.readCount()
	r.offset += uint32(size)
}

func (r *refWalker) walkValueSequence(nbf *NomsBinFormat, cb RefCallback) {
	count := int(r.readCount())
	for i := 0; i < count; i++ {
		r.walkValue(nbf, cb)
	}
}

func (r *refWalker) walkList(nbf *NomsBinFormat, cb RefCallback) {
	r.walkListOrSet(nbf, ListKind, cb)
}

func (r *refWalker) walkSet(nbf *NomsBinFormat, cb RefCallback) {
	r.walkListOrSet(nbf, SetKind, cb)
}

func (r *refWalker) walkListOrSet(nbf *NomsBinFormat, kind NomsKind, cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(nbf, kind, level, cb)
	} else {
		r.walkValueSequence(nbf, cb)
	}
}

func (r *refWalker) walkMap(nbf *NomsBinFormat, cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(nbf, MapKind, level, cb)
	} else {
		r.walkMapLeafSequence(nbf, cb)
	}
}

func (r *refWalker) walkBlob(nbf *NomsBinFormat, cb RefCallback) {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		r.walkMetaSequence(nbf, BlobKind, level, cb)
	} else {
		r.walkBlobLeafSequence()
	}
}

func (r *refWalker) walkMapLeafSequence(nbf *NomsBinFormat, cb RefCallback) {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.walkValue(nbf, cb) // k
		r.walkValue(nbf, cb) // v
	}
}

func (r *refWalker) walkMetaSequence(nbf *NomsBinFormat, k NomsKind, level uint64, cb RefCallback) {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		r.walkRef(nbf, cb) // ref to child sequence
		r.skipOrderedKey(nbf)
		r.skipCount() // numLeaves
	}
}

func (r *refWalker) skipOrderedKey(nbf *NomsBinFormat) {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		r.skipHash()
	default:
		r.walkValue(nbf, func(r Ref) {}) // max Value in subtree reachable from here
	}
}

func (r *refWalker) walkValue(nbf *NomsBinFormat, cb RefCallback) {
	k := r.peekKind()
	switch k {
	case BlobKind:
		r.walkBlob(nbf, cb)
	case BoolKind:
		r.skipKind()
		r.skipBool()
	case FloatKind:
		r.skipKind()
		r.skipFloat(nbf)
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
		r.walkList(nbf, cb)
	case MapKind:
		r.walkMap(nbf, cb)
	case RefKind:
		r.walkRef(nbf, cb)
	case SetKind:
		r.walkSet(nbf, cb)
	case StructKind:
		r.walkStruct(nbf, cb)
	case TupleKind:
		r.walkTuple(nbf, cb)
	case TypeKind:
		r.skipKind()
		r.skipType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		panic("not reachable")
	}
}

func (r *refWalker) walkStruct(nbf *NomsBinFormat, cb RefCallback) {
	walkStruct(nbf, r, cb)
}

func (r *refWalker) walkTuple(nbf *NomsBinFormat, cb RefCallback) {
	walkTuple(nbf, r, cb)
}
