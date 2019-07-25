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
type Uint uint64

// Value interface
func (v Uint) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v Uint) Equals(other Value) bool {
	return v == other
}

func (v Uint) Less(nbf *NomsBinFormat, other LesserValuable) bool {
	if v2, ok := other.(Uint); ok {
		return v < v2
	}
	return UintKind < other.Kind()
}

func (v Uint) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v Uint) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v Uint) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v Uint) typeOf() (*Type, error) {
	return UintType, nil
}

func (v Uint) Kind() NomsKind {
	return UintKind
}

func (v Uint) valueReadWriter() ValueReadWriter {
	return nil
}

func (v Uint) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := UintKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeUint(v)

	return nil
}

func (v Uint) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	// We know the size of the buffer here so allocate it once.
	// UintKind, int (Varint), exp (Varint)
	buff := make([]byte, 1+2*binary.MaxVarintLen64)
	w := binaryNomsWriter{buff, 0}
	err := v.writeTo(&w, nbf)

	if err != nil {
		return nil, err
	}

	return buff[:w.offset], nil
}
