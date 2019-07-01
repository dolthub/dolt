// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

// Float is a Noms Value wrapper around the primitive float64 type.
type Float float64

// Value interface
func (v Float) Value(ctx context.Context) Value {
	return v
}

func (v Float) Equals(format *Format, other Value) bool {
	return v == other
}

func (v Float) Less(f *Format, other LesserValuable) bool {
	if v2, ok := other.(Float); ok {
		return v < v2
	}
	return FloatKind < other.Kind()
}

func (v Float) Hash(f *Format) hash.Hash {
	return getHash(v, f)
}

func (v Float) WalkValues(ctx context.Context, cb ValueCallback) {
}

func (v Float) WalkRefs(cb RefCallback) {
}

func (v Float) typeOf() *Type {
	return FloaTType
}

func (v Float) Kind() NomsKind {
	return FloatKind
}

func (v Float) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Float) writeTo(w nomsWriter, f *Format) {
	FloatKind.writeTo(w, f)
	fl := float64(v)
	if math.IsNaN(fl) || math.IsInf(fl, 0) {
		d.Panic("%f is not a supported number", fl)
	}
	w.writeFloat(v, f)
}

func (v Float) valueBytes(f *Format) []byte {
	// We know the size of the buffer here so allocate it once.
	// FloatKind, int (Varint), exp (Varint)
	buff := make([]byte, 1+2*binary.MaxVarintLen64)
	w := binaryNomsWriter{buff, 0}
	v.writeTo(&w, f)
	return buff[:w.offset]
}
