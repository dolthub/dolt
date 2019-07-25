// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"encoding/binary"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

// Float is a Noms Value wrapper around the primitive float64 type.
type Float float64

// Value interface
func (v Float) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Float) Equals(other Value) bool {
	return v == other
}

func (v Float) Less(nbf *NomsBinFormat, other LesserValuable) bool {
	if v2, ok := other.(Float); ok {
		return v < v2
	}
	return FloatKind < other.Kind()
}

func (v Float) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Float) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Float) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Float) typeOf() (*Type, error) {
	return FloaTType, nil
}

func (v Float) Kind() NomsKind {
	return FloatKind
}

func (v Float) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Float) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := FloatKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeFloat(v, nbf)
	return nil
}

func (v Float) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	// We know the size of the buffer here so allocate it once.
	// FloatKind, int (Varint), exp (Varint)
	buff := make([]byte, 1+2*binary.MaxVarintLen64)
	w := binaryNomsWriter{buff, 0}
	err := v.writeTo(&w, nbf)

	if err != nil {
		return nil, err
	}

	return buff[:w.offset], nil
}
