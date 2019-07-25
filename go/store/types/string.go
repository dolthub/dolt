// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"encoding/binary"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

// String is a Noms Value wrapper around the primitive string type.
type String string

// Value interface
func (s String) Value(ctx context.Context) (Value, error) {
	return s, nil
}

func (s String) Equals(other Value) bool {
	return s == other
}

func (s String) Less(nbf *NomsBinFormat, other LesserValuable) bool {
	if s2, ok := other.(String); ok {
		return s < s2
	}
	return StringKind < other.Kind()
}

func (s String) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(s, nbf)
}

func (s String) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (s String) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (s String) typeOf() (*Type, error) {
	return StringType, nil
}

func (s String) Kind() NomsKind {
	return StringKind
}

func (s String) valueReadWriter() ValueReadWriter {
	return nil
}

func (s String) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	err := StringKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeString(string(s))

	return nil
}

func (s String) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	// We know the size of the buffer here so allocate it once.
	// StringKind, Length (UVarint), UTF-8 encoded string
	buff := make([]byte, 1+binary.MaxVarintLen64+len(s))
	w := binaryNomsWriter{buff, 0}
	err := s.writeTo(&w, nbf)

	if err != nil {
		return nil, err
	}

	return buff[:w.offset], nil
}
