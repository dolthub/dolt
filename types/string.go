// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/hash"

type String struct {
	s string
	h *hash.Hash
}

func NewString(s string) String {
	return String{s, &hash.Hash{}}
}

func (fs String) String() string {
	return fs.s
}

func (s String) hashPointer() *hash.Hash {
	return s.h
}

// Value interface
func (s String) Equals(other Value) bool {
	if other, ok := other.(String); ok {
		return s.s == other.s
	}
	return false
}

func (s String) Less(other Value) bool {
	if s2, ok := other.(String); ok {
		return s.s < s2.s
	}
	return StringKind < other.Type().Kind()
}

func (fs String) Hash() hash.Hash {
	return EnsureHash(fs.h, fs)
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
