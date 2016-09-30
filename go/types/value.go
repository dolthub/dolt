// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/hash"
)

type ValueCallback func(v Value)
type RefCallback func(ref Ref)

// Value is the interface all Noms values implement.
type Value interface {
	// Equals determines if two different Noms values represents the same underlying value.
	Equals(other Value) bool

	// Less determines if this Noms value is less than another Noms value.
	// When comparing two Noms values and both are comparable and the same type (Bool, Number or
	// String) then the natural ordering is used. For other Noms values the Hash of the value is
	// used. When comparing Noms values of different type the following ordering is used:
	// Bool < Number < String < everything else.
	Less(other Value) bool

	// Hash is the hash of the value. All Noms values have a unique hash and if two values have the
	// same hash they must be equal.
	Hash() hash.Hash

	// WalkValues iterates over the immediate children of this value in the DAG, if any, not including
	// Type()
	WalkValues(ValueCallback)

	// WalkRefs iterates over the refs to the underlying chunks. If this value is a collection that has been
	// chunked then this will return the refs of th sub trees of the prolly-tree.
	WalkRefs(RefCallback)

	// Type returns the type of the Noms value. All Noms values carry their runtime Noms type.
	Type() *Type
}

type ValueSlice []Value

func (vs ValueSlice) Len() int           { return len(vs) }
func (vs ValueSlice) Swap(i, j int)      { vs[i], vs[j] = vs[j], vs[i] }
func (vs ValueSlice) Less(i, j int) bool { return vs[i].Less(vs[j]) }
func (vs ValueSlice) Equals(other ValueSlice) bool {
	if vs.Len() != other.Len() {
		return false
	}

	for i, v := range vs {
		if !v.Equals(other[i]) {
			return false
		}
	}

	return true
}
