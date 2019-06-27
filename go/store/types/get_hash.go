// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/liquidata-inc/ld/dolt/go/store/hash"

var getHashOverride func(v Value) hash.Hash

func getHash(v Value) hash.Hash {
	if getHashOverride != nil {
		return getHashOverride(v)
	}
	return getHashNoOverride(v)
}

func getHashNoOverride(v Value) hash.Hash {
	// TODO(binformat)
	return EncodeValue(v, Format_7_18).Hash()
}
