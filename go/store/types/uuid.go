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

func (v UUID) Value(ctx context.Context) (Value, error) {
	return v, nil
}

func (v UUID) Equals(other Value) bool {
	return v == other
}

func (v UUID) Less(nbf *NomsBinFormat, other LesserValuable) bool {
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

func (v UUID) Hash(nbf *NomsBinFormat) (hash.Hash, error) {
	return getHash(v, nbf)
}

func (v UUID) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

func (v UUID) WalkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	return nil
}

func (v UUID) typeOf() (*Type, error) {
	return UUIDType, nil
}

func (v UUID) Kind() NomsKind {
	return UUIDKind
}

func (v UUID) valueReadWriter() ValueReadWriter {
	return nil
}

func (v UUID) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	id := UUID(v)
	byteSl := id[:]
	err := UUIDKind.writeTo(w, nbf)

	if err != nil {
		return err
	}

	w.writeBytes(byteSl)
	return nil
}

func (v UUID) valueBytes(nbf *NomsBinFormat) ([]byte, error) {
	return v[:], nil
}

func (v UUID) String() string {
	return uuid.UUID(v).String()
}
