// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/hash"
)

// Number is a Noms Value wrapper around the primitive float64 type.
type Number float64

// Value interface
func (v Number) Equals(other Value) bool {
	return v == other
}

func (v Number) Less(other Value) bool {
	if v2, ok := other.(Number); ok {
		return v < v2
	}
	return NumberKind < other.Type().Kind()
}

func (v Number) Hash() hash.Hash {
	return getHash(v)
}

func (v Number) ChildValues() []Value {
	return nil
}

func (v Number) Chunks() []Ref {
	return nil
}

func (v Number) Type() *Type {
	return NumberType
}
