// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/hash"
)

// Bool is a Noms Value wrapper around the primitive bool type.
type Bool bool

// Value interface
func (v Bool) Equals(other Value) bool {
	return v == other
}

func (v Bool) Less(other Value) bool {
	if v2, ok := other.(Bool); ok {
		return !bool(v) && bool(v2)
	}
	return true
}

func (v Bool) Hash() hash.Hash {
	return getHash(v)
}

func (v Bool) ChildValues() []Value {
	return nil
}

func (v Bool) Chunks() []Ref {
	return nil
}

func (v Bool) Type() *Type {
	return BoolType
}
