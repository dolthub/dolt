// Copyright 2020 Dolthub, Inc.
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

package types

import (
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

func walkRefs(data []byte, nbf *NomsBinFormat, cb RefCallback) error {
	rw := refWalker{binaryNomsReader{data, 0}}
	return rw.walkValue(nbf, cb)
}

type refWalker struct {
	binaryNomsReader
}

func (r *refWalker) walkRef(nbf *NomsBinFormat, cb RefCallback) error {
	start := r.pos()
	offsets := make([]uint32, refPartEnd)
	offsets[refPartKind] = r.pos()
	r.skipKind()
	offsets[refPartTargetHash] = r.pos()
	r.skipHash()
	offsets[refPartTargetType] = r.pos()
	r.skipTypeInner()
	offsets[refPartHeight] = r.pos()
	r.skipCount()
	end := r.pos()
	ref := Ref{valueImpl{nil, nbf, r.byteSlice(start, end), offsets}}
	return cb(ref)
}

func (r *refWalker) skipTypeInner() {
	k := r.ReadKind()
	switch k {
	case ListKind, RefKind, SetKind, TupleKind, JSONKind:
		r.skipTypeInner()
	case MapKind:
		r.skipTypeInner()
		r.skipTypeInner()
	case StructKind:
		r.skipString()
		count := r.readCount()
		for i := uint64(0); i < count; i++ {
			r.skipString()
		}
		for i := uint64(0); i < count; i++ {
			r.skipTypeInner()
		}
		for i := uint64(0); i < count; i++ {
			r.skipBool()
		}
	case UnionKind:
		l := r.readCount()
		for i := uint64(0); i < l; i++ {
			r.skipTypeInner()
		}
	case CycleKind:
		r.skipString()
	default:
		d.PanicIfFalse(IsPrimitiveKind(k))
	}
}

func (r *refWalker) walkBlobLeafSequence() {
	size := r.readCount()
	r.offset += uint32(size)
}

func (r *refWalker) walkValueSequence(nbf *NomsBinFormat, cb RefCallback) error {
	count := int(r.readCount())
	for i := 0; i < count; i++ {
		if err := r.walkValue(nbf, cb); err != nil {
			return err
		}
	}
	return nil
}

func (r *refWalker) walkMetaSequence(nbf *NomsBinFormat, k NomsKind, level uint64, cb RefCallback) error {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		if err := r.walkRef(nbf, cb); err != nil {
			return err
		}
		if err := r.skipOrderedKey(nbf); err != nil {
			return err
		}
		r.skipCount()
	}
	return nil
}

func (r *refWalker) skipOrderedKey(nbf *NomsBinFormat) error {
	switch r.PeekKind() {
	case hashKind:
		r.skipKind()
		r.skipHash()
	default:
		return r.walkValue(nbf, func(r Ref) error { return nil })
	}
	return nil
}

func (r *refWalker) walkSerialMessage(nbf *NomsBinFormat, cb RefCallback) error {
	sm, err := SerialMessage{}.readFrom(nbf, &r.binaryNomsReader)
	if err != nil {
		return err
	}
	return sm.walkRefs(nbf, cb)
}

func (r *refWalker) walkValue(nbf *NomsBinFormat, cb RefCallback) error {
	k := r.PeekKind()
	switch k {
	case BlobKind:
		r.skipKind()
		level := r.readCount()
		if level > 0 {
			return r.walkMetaSequence(nbf, BlobKind, level, cb)
		}
		r.walkBlobLeafSequence()
		return nil
	case JSONKind:
		r.skipKind()
		return r.walkValue(nbf, cb)
	case ListKind:
		r.skipKind()
		level := r.readCount()
		if level > 0 {
			return r.walkMetaSequence(nbf, ListKind, level, cb)
		}
		return r.walkValueSequence(nbf, cb)
	case MapKind:
		r.skipKind()
		level := r.readCount()
		if level > 0 {
			return r.walkMetaSequence(nbf, MapKind, level, cb)
		}
		count := r.readCount()
		for i := uint64(0); i < count; i++ {
			if err := r.walkValue(nbf, cb); err != nil {
				return err
			}
			if err := r.walkValue(nbf, cb); err != nil {
				return err
			}
		}
		return nil
	case RefKind:
		return r.walkRef(nbf, cb)
	case SetKind:
		r.skipKind()
		level := r.readCount()
		if level > 0 {
			return r.walkMetaSequence(nbf, SetKind, level, cb)
		}
		return r.walkValueSequence(nbf, cb)
	case StructKind:
		r.skipKind()
		r.skipString()
		count := r.readCount()
		for i := uint64(0); i < count; i++ {
			r.skipString()
			if err := r.walkValue(nbf, cb); err != nil {
				return err
			}
		}
		return nil
	case TupleKind:
		r.skipKind()
		count := r.readCount()
		for i := uint64(0); i < count; i++ {
			if err := r.walkValue(nbf, cb); err != nil {
				return err
			}
		}
		return nil
	case SerialMessageKind:
		r.skipKind()
		return r.walkSerialMessage(nbf, cb)
	case TypeKind:
		r.skipKind()
		r.skipTypeInner()
		return nil
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		if IsPrimitiveKind(k) {
			if emptyVal := KindToType[k]; emptyVal != nil {
				r.skipKind()
				emptyVal.skip(nbf, &r.binaryNomsReader)
				return nil
			}
		}
		return ErrUnknownType
	}
	return nil
}

func walkRefs_hash(data []byte, nbf *NomsBinFormat, cb func(h hash.Hash) error) error {
	return walkRefs(data, nbf, func(r Ref) error {
		return cb(r.TargetHash())
	})
}
