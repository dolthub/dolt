package newset

import (
	"math/big"

	"github.com/attic-labs/noms/ref"
)

// Generates fake ascending ref.Ref-s.
type referrator struct {
	count *big.Int
}

func newReferrator() referrator {
	return referrator{big.NewInt(int64(0))}
}

func (r referrator) Next() ref.Ref {
	digest := ref.Sha1Digest{}
	bytes := r.count.Bytes()
	for i := 0; i < len(bytes); i++ {
		digest[len(digest)-i-1] = bytes[len(bytes)-i-1]
	}

	result := ref.New(digest)
	r.count.Add(r.count, big.NewInt(int64(1)))
	return result
}
