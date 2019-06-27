// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
)

type kindAndHash interface {
	Kind() NomsKind
	Hash(*format) hash.Hash
}

func valueLess(v1, v2 kindAndHash) bool {
	switch v2.Kind() {
	case BoolKind, FloatKind, StringKind:
		return false
	default:
		// TODO(binformat)
		return v1.Hash(Format_7_18).Less(v2.Hash(Format_7_18))
	}
}
