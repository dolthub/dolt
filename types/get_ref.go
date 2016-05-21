package types

import "github.com/attic-labs/noms/hash"

var getRefOverride func(v Value) hash.Hash

func getRef(v Value) hash.Hash {
	if getRefOverride != nil {
		return getRefOverride(v)
	}
	return getRefNoOverride(v)
}

func getRefNoOverride(v Value) hash.Hash {
	return EncodeValue(v, nil).Hash()
}

func EnsureRef(r *hash.Hash, v Value) hash.Hash {
	if r.IsEmpty() {
		*r = getRef(v)
	}
	return *r
}
