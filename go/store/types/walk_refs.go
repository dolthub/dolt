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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"

	"github.com/dolthub/dolt/go/store/d"
)

// walkRefs calls cb() on each Ref that can be decoded from |c|. The results
// are precisely equal to DecodeValue(c).walkRefs(cb), but this should be much
// faster.
func walkRefs(data []byte, nbf *NomsBinFormat, cb RefCallback) error {
	rw := newRefWalker(data)
	return rw.walkValue(nbf, cb)
}

type refWalker struct {
	typedBinaryNomsReader
}

func newRefWalker(buff []byte) refWalker {
	nr := binaryNomsReader{buff, 0}
	return refWalker{typedBinaryNomsReader{nr, false}}
}

func (r *refWalker) walkRef(nbf *NomsBinFormat, cb RefCallback) error {
	ref, err := readRef(nbf, &(r.typedBinaryNomsReader))

	if err != nil {
		return err
	}

	return cb(ref)
}

func (r *refWalker) walkSerialMessage(nbf *NomsBinFormat, cb RefCallback) error {
	sm, err := SerialMessage{}.readFrom(nbf, &(r.typedBinaryNomsReader.binaryNomsReader))
	if err != nil {
		return err
	}
	return sm.walkRefs(nbf, cb)
}

func (r *refWalker) walkValue(nbf *NomsBinFormat, cb RefCallback) error {
	k := r.PeekKind()
	switch k {
	case BlobKind, ListKind, MapKind, SetKind, StructKind:
		return fmt.Errorf("unsupported kind: %s", k)
	case RefKind:
		return r.walkRef(nbf, cb)
	case SerialMessageKind:
		r.skipKind()
		return r.walkSerialMessage(nbf, cb)
	case TypeKind:
		r.skipKind()
		return r.skipType()
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

