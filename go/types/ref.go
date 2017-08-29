// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/hash"

type Ref struct {
	target     hash.Hash
	targetType *Type
	height     uint64
	h          *hash.Hash
}

func NewRef(v Value) Ref {
	// TODO: Taking the hash will duplicate the work of computing the type
	return Ref{v.Hash(), TypeOf(v), maxChunkHeight(v) + 1, &hash.Hash{}}
}

// ToRefOfValue returns a new Ref that points to the same target as |r|, but
// with the type 'Ref<Value>'.
func ToRefOfValue(r Ref) Ref {
	return Ref{r.TargetHash(), ValueType, r.Height(), &hash.Hash{}}
}

// Constructs a Ref directly from struct properties. This should not be used outside decoding and testing within the types package.
func constructRef(target hash.Hash, targetType *Type, height uint64) Ref {
	return Ref{target, targetType, height, &hash.Hash{}}
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

func (r Ref) TargetType() *Type {
	return r.targetType
}

// Value interface
func (r Ref) Value() Value {
	return r
}

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

func (r Ref) typeOf() *Type {
	return makeCompoundType(RefKind, r.targetType)
}

func (r Ref) Kind() NomsKind {
	return RefKind
}
