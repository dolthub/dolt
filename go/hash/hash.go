// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package hash implements the hash function used throughout Noms.
//
// Noms serialization from version 5-onward uses the first 20 bytes of sha-512 for hashes.
//
// sha-512 was chosen because:
//
// - sha-1 is no longer recommended.
// - sha-3 is brand new, not a lot of platform support.
// - blake is not commonly used, not a lot of platform support.
// - within sha-2, sha-512 is faster than sha-256 on 64 bit.
//
// Our specific truncation scheme (first 20 bytes) was chosen because:
//
// - The "standard" truncation schemes are not widely supported. For example, at time of writing, there is no fast native implementation of sha512/256 on Node.
// - The smallest standard truncation of sha512 is 28 bytes, but we don't need this many. And because we are a database, the size of the hashes matters. Bigger hashes mean less data in each chunk, which means less tree fan-out, which means slower iteration and searching. 20 bytes is a good balance between collision resistance and wide trees.
// - 20 bytes leads to a nice round number of base32 digits: 32.
//
// The textual serialization of hashes uses big-endian base32 with the alphabet {0-9,a-v}. This scheme was chosen because:
//
// - It's easy to convert to and from base32 without bignum arithemetic.
// - No special chars: you can double-click to select in GUIs.
// - Sorted hashes will be sorted textually, making it easy to scan for humans.
//
// In Noms, the hash function is a component of the serialization version, which is constant over the entire lifetime of a single database. So clients do not need to worry about encountering multiple hash functions in the same database.
package hash

import (
	"bytes"
	"crypto/sha512"
	"fmt"
	"regexp"
	"strconv"

	"github.com/attic-labs/noms/go/d"
)

const (
	ByteLen   = 20
	StringLen = 32 // 20 * 8 / log2(32)
)

var (
	pattern   = regexp.MustCompile("^([0-9a-v]{" + strconv.Itoa(StringLen) + "})$")
	emptyHash = Hash{}
)

type Hash struct {
	digest Digest
}

type Digest [ByteLen]byte

// Digest returns a *copy* of the digest that backs Hash.
func (r Hash) Digest() Digest {
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
	return encode(r.digest[:])
}

func New(digest Digest) Hash {
	return Hash{digest}
}

func FromData(data []byte) Hash {
	r := sha512.Sum512(data)
	d := Digest{}
	copy(d[:], r[:ByteLen])
	return New(d)
}

// FromSlice creates a new Hash backed by data, ensuring that data is an acceptable length.
func FromSlice(data []byte) Hash {
	d.Chk.True(len(data) == ByteLen)
	digest := Digest{}
	copy(digest[:], data)
	return New(digest)
}

func MaybeParse(s string) (Hash, bool) {
	match := pattern.FindStringSubmatch(s)
	if match == nil {
		return emptyHash, false
	}
	return FromSlice(decode(s)), true
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
