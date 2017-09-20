// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package hash implements the hash function used throughout Noms.
//
// Noms serialization from version 4-onward uses the first 20 bytes of sha-512 for hashes.
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
	// ByteLen is the number of bytes used to represent the Hash.
	ByteLen = 20

	// StringLen is the number of characters need to represent the Hash using Base32.
	StringLen = 32 // 20 * 8 / log2(32)
)

var (
	pattern   = regexp.MustCompile("^([0-9a-v]{" + strconv.Itoa(StringLen) + "})$")
	emptyHash = Hash{}
)

// Hash is used to represent the hash of a Noms Value.
type Hash [ByteLen]byte

// IsEmpty determines if this Hash is equal to the empty hash (all zeroes).
func (h Hash) IsEmpty() bool {
	return h == emptyHash
}

// String returns a string representation of the hash using Base32 encoding.
func (h Hash) String() string {
	return encode(h[:])
}

// Of computes a new Hash from data.
func Of(data []byte) Hash {
	r := sha512.Sum512(data)
	h := Hash{}
	copy(h[:], r[:ByteLen])
	return h
}

// New creates a new Hash backed by data, ensuring that data is an acceptable length.
func New(data []byte) Hash {
	d.PanicIfFalse(len(data) == ByteLen)
	h := Hash{}
	copy(h[:], data)
	return h
}

// MaybeParse parses a string representing a hash as a Base32 encoded byte array.
// If the string is not well formed then this returns (emptyHash, false).
func MaybeParse(s string) (Hash, bool) {
	match := pattern.FindStringSubmatch(s)
	if match == nil {
		return emptyHash, false
	}
	return New(decode(s)), true
}

// Parse parses a string representing a hash as a Base32 encoded byte array.
// If the string is not well formed then this panics.
func Parse(s string) Hash {
	r, ok := MaybeParse(s)
	if !ok {
		d.PanicIfError(fmt.Errorf("Cound not parse Hash: %s", s))
	}
	return r
}

// Less compares two hashes returning whether this Hash is less than other.
func (h Hash) Less(other Hash) bool {
	return bytes.Compare(h[:], other[:]) < 0
}

// Greater compares two hashes returning whether this Hash is greater than other.
func (h Hash) Greater(other Hash) bool {
	// TODO: Remove this
	return bytes.Compare(h[:], other[:]) > 0
}

// HashSet is a set of Hashes.
type HashSet map[Hash]struct{}

func NewHashSet(hashes ...Hash) HashSet {
	out := make(HashSet, len(hashes))
	for _, h := range hashes {
		out.Insert(h)
	}
	return out
}

// Insert adds a Hash to the set.
func (hs HashSet) Insert(hash Hash) {
	hs[hash] = struct{}{}
}

// Has returns true if the HashSet contains hash.
func (hs HashSet) Has(hash Hash) (has bool) {
	_, has = hs[hash]
	return
}

// Remove removes hash from the HashSet.
func (hs HashSet) Remove(hash Hash) {
	delete(hs, hash)
}
