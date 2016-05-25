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

func EnsureHash(h *hash.Hash, v Value) hash.Hash {
	if h.IsEmpty() {
		*h = getHash(v)
	}
	return *h
}

type hashCacher interface {
	hashPointer() *hash.Hash
}

func assignHash(hc hashCacher, h hash.Hash) {
	*hc.hashPointer() = h
}
