package ref

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
	"regexp"

	. "github.com/attic-labs/noms/dbg"
)

var (
	// In the future we will allow different digest types, so this will get more complicated. For now sha1 is fine.
	pattern = regexp.MustCompile("^sha1-([0-9a-f]{40})$")
)

type Sha1Digest [sha1.Size]byte

type Ref struct {
	// In the future, we will also store the algorithm, and digest will thus probably have to be a slice (because it can vary in size)
	digest Sha1Digest
}

// Digest returns a *copy* of the digest that backs Ref.
func (r Ref) Digest() Sha1Digest {
	return r.digest
}

func (r Ref) String() string {
	return fmt.Sprintf("sha1-%s", hex.EncodeToString(r.digest[:]))
}

func New(digest Sha1Digest) Ref {
	return Ref{digest}
}

// NewHash creates a new instance of the hash we use for refs.
func NewHash() hash.Hash {
	return sha1.New()
}

func FromHash(h hash.Hash) Ref {
	Chk.Equal(sha1.Size, h.Size())
	digest := Sha1Digest{}
	h.Sum(digest[:0])
	return New(digest)
}

func Parse(s string) (r Ref, err error) {
	match := pattern.FindStringSubmatch(s)
	if match == nil {
		return r, fmt.Errorf("Could not parse ref: %s", s)
	}

	// TODO: The new temp byte array is kinda bummer. Would be better to walk the string and decode each byte into result.digest. But can't find stdlib functions to do that.
	n, err := hex.Decode(r.digest[:], []byte(match[1]))
	if err != nil {
		return
	}

	// If there was no error, we should have decoded exactly one digest worth of bytes.
	Chk.Equal(sha1.Size, n)
	return
}

func MustParse(s string) Ref {
	r, err := Parse(s)
	Chk.NoError(err)
	return r
}

// Less compares two Refs, returning true if the first is less than the second.
// This can be called a lot, so performance and avoiding creating garbage may be important.
// Particularly, Chk.Equals{Value} does reflection, and this can be expensive, so avoid it here.
func Less(r1, r2 Ref) bool {
	d1, d2 := r1.digest, r2.digest
	Chk.True(len(d1) == len(d2)) // Hot path!
	for k := 0; k < len(d1); k++ {
		b1, b2 := d1[k], d2[k]
		if b1 < b2 {
			return true
		} else if b1 > b2 {
			return false
		}
	}
	return false
}
