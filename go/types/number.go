// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"encoding/binary"
	"math"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

// Number is a Noms Value wrapper around the primitive float64 type.
type Number float64

// Value interface
func (v Number) Value() Value {
	return v
}

func (v Number) Equals(other Value) bool {
	return v == other
}

func (v Number) Less(other Value) bool {
	if v2, ok := other.(Number); ok {
		return v < v2
	}
	return NumberKind < other.Kind()
}

func (v Number) Hash() hash.Hash {
	return getHash(v)
}

func (v Number) WalkValues(cb ValueCallback) {
}

func (v Number) WalkRefs(cb RefCallback) {
}

func (v Number) typeOf() *Type {
	return NumberType
}

func (v Number) Kind() NomsKind {
	return NumberKind
}

func (v Number) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Number) writeTo(w nomsWriter) {
	NumberKind.writeTo(w)
	f := float64(v)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		d.Panic("%f is not a supported number", f)
	}
	w.writeNumber(v)
}

func (v Number) valueBytes() []byte {
	// We know the size of the buffer here so allocate it once.
	// NumberKind, int (Varint), exp (Varint)
	buff := make([]byte, 1+2*binary.MaxVarintLen64)
	w := binaryNomsWriter{buff, 0}
	v.writeTo(&w)
	return buff[:w.offset]
}
