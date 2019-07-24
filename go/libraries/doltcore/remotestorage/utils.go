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

import "github.com/liquidata-inc/dolt/go/store/hash"

// HashesToSlices takes a list of hashes and converts each hash to a byte slice returning a slice of byte slices
func HashesToSlices(hashes []hash.Hash) [][]byte {
	slices := make([][]byte, len(hashes))

	for i, h := range hashes {
		tmp := h
		slices[i] = tmp[:]
	}

	return slices
}

// HashSetToSlices takes a HashSet and converts it to a slice of hashes, and a slice of byte slices
func HashSetToSlices(hashes hash.HashSet) ([]hash.Hash, [][]byte) {
	hashSl := make([]hash.Hash, len(hashes))
	bytesSl := make([][]byte, len(hashes))

	i := 0
	for h := range hashes {
		tmp := h
		hashSl[i] = tmp
		bytesSl[i] = tmp[:]
		i++
	}

	return hashSl, bytesSl
}

// ParseByteSlices takes a slice of byte slices and converts it to a HashSet, and a map from hash to it's index in the
// original slice
func ParseByteSlices(byteSlices [][]byte) (hash.HashSet, map[hash.Hash]int) {
	hs := make(hash.HashSet)
	hashToIndex := make(map[hash.Hash]int)

	for i, byteSl := range byteSlices {
		h := hash.New(byteSl)
		hs[h] = struct{}{}
		hashToIndex[h] = i
	}

	return hs, hashToIndex
}
