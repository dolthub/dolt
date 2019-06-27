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
func (s String) Value(ctx context.Context) Value {
	return s
}

func (s String) Equals(other Value) bool {
	return s == other
}

func (s String) Less(f *Format, other LesserValuable) bool {
	if s2, ok := other.(String); ok {
		return s < s2
	}
	return StringKind < other.Kind()
}

func (s String) Hash(f *Format) hash.Hash {
	return getHash(s, f)
}

func (s String) WalkValues(ctx context.Context, cb ValueCallback) {
}

func (s String) WalkRefs(cb RefCallback) {
}

func (s String) typeOf() *Type {
	return StringType
}

func (s String) Kind() NomsKind {
	return StringKind
}

func (s String) valueReadWriter() ValueReadWriter {
	return nil
}

func (s String) writeTo(w nomsWriter, f *Format) {
	StringKind.writeTo(w, f)
	w.writeString(string(s))
}

func (s String) valueBytes(f *Format) []byte {
	// We know the size of the buffer here so allocate it once.
	// StringKind, Length (UVarint), UTF-8 encoded string
	buff := make([]byte, 1+binary.MaxVarintLen64+len(s))
	w := binaryNomsWriter{buff, 0}
	s.writeTo(&w, f)
	return buff[:w.offset]
}
