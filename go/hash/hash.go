// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package hash

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"

	"github.com/attic-labs/noms/go/d"
)

var (
	// In the future we will allow different digest types, so this will get more complicated. For now sha1 is fine.
	pattern   = regexp.MustCompile("^sha1-([0-9a-f]{40})$")
	emptyHash = Hash{}
)

type Sha1Digest [sha1.Size]byte

type Hash struct {
	// In the future, we will also store the algorithm, and digest will thus probably have to be a slice (because it can vary in size)
	digest Sha1Digest
}

// Digest returns a *copy* of the digest that backs Hash.
func (r Hash) Digest() Sha1Digest {
	return r.digest
}

func (r Hash) IsEmpty() bool {
	return r.digest == emptyHash.digest
}

// DigestSlice returns a slice of the digest that backs A NEW COPY of Hash, because the receiver of this method is not a pointer.
func (r Hash) DigestSlice() []byte {
	return r.digest[:]
}

func (r Hash) String() string {
	return fmt.Sprintf("sha1-%s", hex.EncodeToString(r.digest[:]))
}

func New(digest Sha1Digest) Hash {
	return Hash{digest}
}

func FromData(data []byte) Hash {
	return New(sha1.Sum(data))
}

// FromSlice creates a new Hash backed by data, ensuring that data is an acceptable length.
func FromSlice(data []byte) Hash {
	d.Chk.True(len(data) == sha1.Size)
	digest := Sha1Digest{}
	copy(digest[:], data)
	return New(digest)
}

func MaybeParse(s string) (r Hash, ok bool) {
	match := pattern.FindStringSubmatch(s)
	if match == nil {
		return
	}

	// TODO: The new temp byte array is kinda bummer. Would be better to walk the string and decode each byte into result.digest. But can't find stdlib functions to do that.
	n, err := hex.Decode(r.digest[:], []byte(match[1]))
	d.Chk.NoError(err) // The regexp above should have validated the input

	// If there was no error, we should have decoded exactly one digest worth of bytes.
	d.Chk.True(sha1.Size == n)
	ok = true
	return
}

func Parse(s string) Hash {
	r, ok := MaybeParse(s)
	if !ok {
		d.PanicIfError(fmt.Errorf("Cound not parse Hash: %s", s))
	}
	return r
}

func (r Hash) Less(other Hash) bool {
	return bytes.Compare(r.digest[:], other.digest[:]) < 0
}

func (r Hash) Greater(other Hash) bool {
	return bytes.Compare(r.digest[:], other.digest[:]) > 0
}
