// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/hash"

var getHashOverride func(v Value) hash.Hash

func getHash(v Value) hash.Hash {
	if getHashOverride != nil {
		return getHashOverride(v)
	}
	return getHashNoOverride(v)
}

func getHashNoOverride(v Value) hash.Hash {
	return EncodeValue(v).Hash()
}

func EnsureHash(h *hash.Hash, v Value) hash.Hash {
	if h.IsEmpty() {
		*h = getHash(v)
	}
	return *h
}
