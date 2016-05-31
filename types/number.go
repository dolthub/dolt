// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/hash"
)

type Number float64

// Value interface
func (v Number) Equals(other Value) bool {
	return v == other
}

func (v Number) Less(other Value) bool {
	if v2, ok := other.(Number); ok {
		return float64(v) < float64(v2)
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

// ValueWriter - primitive interface
func (v Number) ToPrimitive() interface{} {
	return float64(v)
}
