// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

type Ref struct {
	target hash.Hash
	height uint64
	t      *Type
	h      *hash.Hash
}

func NewRef(v Value) Ref {
	return Ref{v.Hash(), maxChunkHeight(v) + 1, MakeRefType(v.Type()), &hash.Hash{}}
}

// Constructs a Ref directly from struct properties. This should not be used outside decoding and testing within the types package.
func constructRef(t *Type, target hash.Hash, height uint64) Ref {
	d.PanicIfFalse(RefKind == t.Kind())
	d.PanicIfFalse(ValueType != t.Desc.(CompoundDesc).ElemTypes[0])
	return Ref{target, height, t, &hash.Hash{}}
}

func maxChunkHeight(v Value) (max uint64) {
	v.WalkRefs(func(r Ref) {
		if height := r.Height(); height > max {
			max = height
		}
	})
	return
}

func (r Ref) TargetHash() hash.Hash {
	return r.target
}

func (r Ref) Height() uint64 {
	return r.height
}

func (r Ref) TargetValue(vr ValueReader) Value {
	return vr.ReadValue(r.target)
}

// Value interface
func (r Ref) Equals(other Value) bool {
	return r.Hash() == other.Hash()
}

func (r Ref) Less(other Value) bool {
	return valueLess(r, other)
}

func (r Ref) Hash() hash.Hash {
	if r.h.IsEmpty() {
		*r.h = getHash(r)
	}

	return *r.h
}

func (r Ref) WalkValues(cb ValueCallback) {
}

func (r Ref) WalkRefs(cb RefCallback) {
	cb(r)
}

func (r Ref) Type() *Type {
	return r.t
}
