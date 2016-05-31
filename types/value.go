// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/hash"
)

// Value is implemented by every noms value
type Value interface {
	Equals(other Value) bool
	Less(other Value) bool

	Hash() hash.Hash
	// Returns the immediate children of this value in the DAG, if any, not including Type().
	ChildValues() []Value
	Chunks() []Ref
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
