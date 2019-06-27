package types

import (
	"context"

	"github.com/google/uuid"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

const (
	uuidNumBytes = 16
)

type UUID uuid.UUID

func (v UUID) Value(ctx context.Context) Value {
	return v
}

func (v UUID) Equals(other Value) bool {
	return v == other
}

func (v UUID) Less(other LesserValuable) bool {
	if v2, ok := other.(UUID); ok {
		for i := 0; i < uuidNumBytes; i++ {
			b1 := v[i]
			b2 := v2[i]

			if b1 != b2 {
				return b1 < b2
			}
		}

		return false
	}
	return UUIDKind < other.Kind()
}

func (v UUID) Hash(f *format) hash.Hash {
	return getHash(v, f)
}

func (v UUID) WalkValues(ctx context.Context, cb ValueCallback) {
}

func (v UUID) WalkRefs(cb RefCallback) {
}

func (v UUID) typeOf() *Type {
	return UUIDType
}

func (v UUID) Kind() NomsKind {
	return UUIDKind
}

func (v UUID) valueReadWriter() ValueReadWriter {
	return nil
}

func (v UUID) writeTo(w nomsWriter, f *format) {
	id := UUID(v)
	byteSl := id[:]
	UUIDKind.writeTo(w, f)
	w.writeBytes(byteSl)
}

func (v UUID) valueBytes(f *format) []byte {
	return v[:]
}

func (v UUID) String() string {
	return uuid.UUID(v).String()
}
