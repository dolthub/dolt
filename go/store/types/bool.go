// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

// Bool is a Noms Value wrapper around the primitive bool type.
type Bool bool

// Value interface
func (b Bool) Value(ctx context.Context) (Value, error) {
	return b, nil
}

func (b Bool) Equals(other Value) bool {
	return b == other
}

func (b Bool) Less(nbf *NomsBinFormat, other LesserValuable) bool {
	if b2, ok := other.(Bool); ok {
		return !bool(b) && bool(b2)
	}
	return true
}

func (b Bool) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(b, nbf)
}

func (b Bool) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (b Bool) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (b Bool) typeOf() (*Type, error) {
	return BoolType, nil
}

func (b Bool) Kind() NomsKind {
	return BoolKind
}

func (b Bool) valueReadWriter() ValueReadWriter {
	return nil
}

func (b Bool) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := BoolKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeBool(bool(b))

	return nil
}

func (b Bool) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	if bool(b) {
		return []byte{byte(BoolKind), 1}, nil
	}
	return []byte{byte(BoolKind), 0}, nil
}
