package ref

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
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

func (r Ref) Equals(other *Ref) bool {
	return r.digest == other.digest
}

func (r Ref) String() string {
	return fmt.Sprintf("sha1-%s", hex.EncodeToString(r.digest[:]))
}

func New(digest Sha1Digest) Ref {
	return Ref{digest}
}

func Parse(s string) (*Ref, error) {
	match := pattern.FindStringSubmatch(s)
	if match == nil {
		return nil, fmt.Errorf("Could not parse ref: %s", s)
	}
	result := Ref{}

	// TODO: The new temp byte array is kinda bummer. Would be better to walk the string and decode each byte into result.digest. But can't find stdlib functions to do that.
	n, err := hex.Decode(result.digest[:], []byte(match[1]))
	if err != nil {
		return nil, err
	}

	// If there was no error, we should have decoded exactly one digest worth of bytes.
	Chk.Equal(sha1.Size, n)
	return &result, nil
}

func MustParse(s string) *Ref {
	r, err := Parse(s)
	Chk.NoError(err)
	return r
}
