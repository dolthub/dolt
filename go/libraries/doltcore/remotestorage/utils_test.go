// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remotestorage

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

func TestHashesToSlices(t *testing.T) {
	const numHashes = 32

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var randomHashes []hash.Hash
	var randomHashBytes [][]byte
	for i := 0; i < numHashes; i++ {
		var h hash.Hash

		for j := 0; j < len(h); j++ {
			h[j] = byte(rng.Intn(255))
		}

		randomHashes = append(randomHashes, h)
		randomHashBytes = append(randomHashBytes, h[:])
	}

	var zeroHash hash.Hash
	tests := []struct {
		name     string
		in       []hash.Hash
		expected [][]byte
	}{
		{
			"test nil",
			nil,
			[][]byte{},
		},
		{
			"test empty",
			[]hash.Hash{},
			[][]byte{},
		},
		{
			"test one hash",
			[]hash.Hash{zeroHash},
			[][]byte{zeroHash[:]},
		},
		{
			"test many random hashes",
			randomHashes,
			randomHashBytes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := HashesToSlices(test.in)

			if !reflect.DeepEqual(test.expected, actual) {
				t.Error("unexpected result")
			}
		})
	}
}

func TestHashSetToSlices(t *testing.T) {
	const numHashes = 32

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	randomHashSet := make(hash.HashSet)

	var randomHashes []hash.Hash
	var randomHashBytes [][]byte
	for i := 0; i < numHashes; i++ {
		var h hash.Hash

		for j := 0; j < len(h); j++ {
			h[j] = byte(rng.Intn(255))
		}

		randomHashSet.Insert(h)
		randomHashes = append(randomHashes, h)
		randomHashBytes = append(randomHashBytes, h[:])
	}

	var zeroHash hash.Hash
	tests := []struct {
		name           string
		hashes         hash.HashSet
		expectedHashes []hash.Hash
		expectedBytes  [][]byte
	}{
		{
			"test nil",
			nil,
			[]hash.Hash{},
			[][]byte{},
		},
		{
			"test empty",
			hash.HashSet{},
			[]hash.Hash{},
			[][]byte{},
		},
		{
			"test one hash",
			hash.HashSet{zeroHash: struct{}{}},
			[]hash.Hash{zeroHash},
			[][]byte{zeroHash[:]},
		},
		{
			"test many random hashes",
			randomHashSet,
			randomHashes,
			randomHashBytes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hashes, bytes := HashSetToSlices(test.hashes)

			if len(hashes) != len(test.hashes) || len(bytes) != len(test.hashes) {
				t.Error("unexpected size")
			}

			for i := 0; i < len(test.hashes); i++ {
				h, hBytes := hashes[i], bytes[i]

				if !test.hashes.Has(h) {
					t.Error("missing hash")
				}

				if !reflect.DeepEqual(h[:], hBytes) {
					t.Error("unexpected bytes")
				}
			}
		})
	}
}

func TestParseByteSlices(t *testing.T) {
	const numHashes = 32

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var randomHashBytes [][]byte
	for i := 0; i < numHashes; i++ {
		var h hash.Hash

		for j := 0; j < len(h); j++ {
			h[j] = byte(rng.Intn(255))
		}

		randomHashBytes = append(randomHashBytes, h[:])
	}

	var zeroHash hash.Hash
	tests := []struct {
		name  string
		bytes [][]byte
	}{
		{
			"test nil",
			[][]byte{},
		},
		{
			"test empty",
			[][]byte{},
		},
		{
			"test one hash",
			[][]byte{zeroHash[:]},
		},
		{
			"test many random hashes",
			randomHashBytes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hashes, hashToIndex := ParseByteSlices(test.bytes)

			if len(hashes) != len(test.bytes) {
				t.Error("unexpected size")
			}

			for h := range hashes {
				idx := hashToIndex[h]

				if !reflect.DeepEqual(test.bytes[idx], h[:]) {
					t.Error("unexpected value")
				}
			}
		})
	}

}
