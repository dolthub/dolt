// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/hash"

type String string

// Value interface
func (s String) Equals(other Value) bool {
	return s == other
}

func (s String) Less(other Value) bool {
	if s2, ok := other.(String); ok {
		return s < s2
	}
	return StringKind < other.Type().Kind()
}

func (s String) Hash() hash.Hash {
	return getHash(s)
}

func (fs String) ChildValues() []Value {
	return nil
}

func (fs String) Chunks() []Ref {
	return nil
}

func (fs String) Type() *Type {
	return StringType
}
