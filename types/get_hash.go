package types

import "github.com/attic-labs/noms/hash"

var getHashOverride func(v Value) hash.Hash

func getHash(v Value) hash.Hash {
	if getHashOverride != nil {
		return getHashOverride(v)
	}
	return getHashNoOverride(v)
}

func getHashNoOverride(v Value) hash.Hash {
	return EncodeValue(v, nil).Hash()
}

func EnsureHash(r *hash.Hash, v Value) hash.Hash {
	if r.IsEmpty() {
		*r = getHash(v)
	}
	return *r
}
