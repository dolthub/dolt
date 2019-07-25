// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"encoding/binary"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

// Int is a Noms Value wrapper around the primitive int32 type.
type Int int64

// Value interface
func (v Int) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Int) Equals(other Value) bool {
	return v == other
}

func (v Int) Less(nbf *NomsBinFormat, other LesserValuable) bool {
	if v2, ok := other.(Int); ok {
		return v < v2
	}
	return IntKind < other.Kind()
}

func (v Int) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Int) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Int) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Int) typeOf() (*Type, error) {
	return IntType, nil
}

func (v Int) Kind() NomsKind {
	return IntKind
}

func (v Int) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Int) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := IntKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeInt(v)

	return nil
}

func (v Int) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	// We know the size of the buffer here so allocate it once.
	// IntKind, int (Varint), exp (Varint)
	buff := make([]byte, 1+2*binary.MaxVarintLen64)
	w := binaryNomsWriter{buff, 0}
	err := v.writeTo(&w, nbf)

	if err != nil {
		return nil, err
	}

	return buff[:w.offset], err
}
