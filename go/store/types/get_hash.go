// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/liquidata-inc/ld/dolt/go/store/hash"

var getHashOverride func(v Value) hash.Hash

func getHash(v Value, nbf *NomsBinFormat) hash.Hash {
	if getHashOverride != nil {
		return getHashOverride(v)
	}
	return getHashNoOverride(v, nbf)
}

func getHashNoOverride(v Value, nbf *NomsBinFormat) hash.Hash {
	return EncodeValue(v, nbf).Hash()
}
