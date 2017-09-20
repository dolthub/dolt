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
func (b Bool) Value() Value {
	return b
}

func (b Bool) Equals(other Value) bool {
	return b == other
}

func (b Bool) Less(other Value) bool {
	if b2, ok := other.(Bool); ok {
		return !bool(b) && bool(b2)
	}
	return true
}

func (b Bool) Hash() hash.Hash {
	return getHash(b)
}

func (b Bool) WalkValues(cb ValueCallback) {
}

func (b Bool) WalkRefs(cb RefCallback) {
}

func (b Bool) typeOf() *Type {
	return BoolType
}

func (b Bool) Kind() NomsKind {
	return BoolKind
}

func (b Bool) valueReadWriter() ValueReadWriter {
	return nil
}

func (b Bool) writeTo(w nomsWriter) {
	BoolKind.writeTo(w)
	w.writeBool(bool(b))
}

func (b Bool) valueBytes() []byte {
	if bool(b) {
		return []byte{byte(BoolKind), 1}
	}
	return []byte{byte(BoolKind), 0}
}
