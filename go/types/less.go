// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/hash"
)

type kindAndHash interface {
	Kind() NomsKind
	Hash() hash.Hash
}

func valueLess(v1, v2 kindAndHash) bool {
	switch v2.Kind() {
	case BoolKind, NumberKind, StringKind:
		return false
	default:
		return v1.Hash().Less(v2.Hash())
	}
}
