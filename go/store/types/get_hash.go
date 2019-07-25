// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/liquidata-inc/ld/dolt/go/store/hash"

var getHashOverride func(v Value) (hash.Hash, error)

func getHash(v Value, nbf *NomsBinFormat) (hash.Hash, error) {
	if getHashOverride != nil {
		return getHashOverride(v)
	}
	return getHashNoOverride(v, nbf)
}

func getHashNoOverride(v Value, nbf *NomsBinFormat) (hash.Hash, error) {
	val, err := EncodeValue(v, nbf)

	if err != nil {
		return hash.Hash{}, err
	}

	return val.Hash(), nil
}
