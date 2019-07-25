// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

type kindAndHash interface {
	Kind() NomsKind
	Hash(*NomsBinFormat) (hash.Hash, error)
}

func valueLess(nbf *NomsBinFormat, v1, v2 kindAndHash) (bool, error) {
	switch v2.Kind() {
	case UnknownKind:
		return false, ErrUnknownType

	case BoolKind, FloatKind, StringKind:
		return false, nil

	default:
		h1, err := v1.Hash(nbf)

		if err != nil {
			return false, err
		}

		h2, err := v2.Hash(nbf)

		if err != nil {
			return false, err
		}

		return h1.Less(h2), nil
	}
}
