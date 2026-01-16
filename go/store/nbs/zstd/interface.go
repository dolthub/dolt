// Copyright 2026 Dolthub, Inc.
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

package zstd

// Based on the build tag |zstd_native| either the CGO-based (github.com/dolthub/gozstd) or native
// implementation (github.com/klauspost/compress/zstd) will be used.
//
// This is a runtime flag |BuildDictEnabled| indicating whether the runtime can create new dictionaries.
// In the event that it cannot, the build in static dictionaries should be used.

// CDict - compression dictionary
type CDict struct {
	impl interface{}
}

// DDict - decompression dictionary
type DDict struct {
	impl interface{}
}

type Compressor interface {
	Compress(dst, src []byte) []byte

	Decompress(dst, src []byte) ([]byte, error)

	CompressDict(dst, src []byte, dict *CDict) ([]byte, error)

	DecompressDict(dst, src []byte, dict *DDict) ([]byte, error)

	NewCDict(dict []byte) (*CDict, error)

	NewDDict(dict []byte) (*DDict, error)

	BuildDict(samples [][]byte, dictSize int) []byte
}

var compressor Compressor

func init() {
	compressor = createDefaultCompressor()
}

// Compress compresses data using the default compressor
func Compress(dst, src []byte) []byte {
	return compressor.Compress(dst, src)
}

// Decompress decompresses data using the default compressor
func Decompress(dst, src []byte) ([]byte, error) {
	return compressor.Decompress(dst, src)
}

// CompressDict compresses data using a compression dictionary with the default compressor
func CompressDict(dst, src []byte, dict *CDict) ([]byte, error) {
	return compressor.CompressDict(dst, src, dict)
}

// DecompressDict decompresses data using a decompression dictionary with the default compressor
func DecompressDict(dst, src []byte, dict *DDict) ([]byte, error) {
	return compressor.DecompressDict(dst, src, dict)
}

// NewCDict creates a new compression dictionary using the default compressor
func NewCDict(dict []byte) (*CDict, error) {
	return compressor.NewCDict(dict)
}

// NewDDict creates a new decompression dictionary using the default compressor
func NewDDict(dict []byte) (*DDict, error) {
	return compressor.NewDDict(dict)
}

// BuildDict builds a dictionary from training samples using the default compressor
func BuildDict(samples [][]byte, dictSize int) []byte {
	return compressor.BuildDict(samples, dictSize)
}
