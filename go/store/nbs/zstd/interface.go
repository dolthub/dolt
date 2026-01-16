// Copyright 2024 Dolthub, Inc.
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

// CDict represents a compression dictionary
type CDict struct {
	impl interface{}
}

// DDict represents a decompression dictionary  
type DDict struct {
	impl interface{}
}

// Compressor provides zstd compression functionality
type Compressor interface {
	// Compress compresses data using zstd
	Compress(dst, src []byte) []byte
	
	// Decompress decompresses data using zstd
	Decompress(dst, src []byte) ([]byte, error)
	
	// CompressDict compresses data using a compression dictionary
	CompressDict(dst, src []byte, dict *CDict) []byte
	
	// DecompressDict decompresses data using a decompression dictionary
	DecompressDict(dst, src []byte, dict *DDict) ([]byte, error)
	
	// NewCDict creates a new compression dictionary
	NewCDict(dict []byte) (*CDict, error)
	
	// NewDDict creates a new decompression dictionary
	NewDDict(dict []byte) (*DDict, error)
	
	// BuildDict builds a dictionary from training samples
	BuildDict(samples [][]byte, dictSize int) []byte
}

var DefaultCompressor Compressor

func init() {
	DefaultCompressor = createDefaultCompressor()
}

// Compress compresses data using the default compressor
func Compress(dst, src []byte) []byte {
	return DefaultCompressor.Compress(dst, src)
}

// Decompress decompresses data using the default compressor
func Decompress(dst, src []byte) ([]byte, error) {
	return DefaultCompressor.Decompress(dst, src)
}

// CompressDict compresses data using a compression dictionary with the default compressor
func CompressDict(dst, src []byte, dict *CDict) []byte {
	return DefaultCompressor.CompressDict(dst, src, dict)
}

// DecompressDict decompresses data using a decompression dictionary with the default compressor
func DecompressDict(dst, src []byte, dict *DDict) ([]byte, error) {
	return DefaultCompressor.DecompressDict(dst, src, dict)
}

// NewCDict creates a new compression dictionary using the default compressor
func NewCDict(dict []byte) (*CDict, error) {
	return DefaultCompressor.NewCDict(dict)
}

// NewDDict creates a new decompression dictionary using the default compressor
func NewDDict(dict []byte) (*DDict, error) {
	return DefaultCompressor.NewDDict(dict)
}

// BuildDict builds a dictionary from training samples using the default compressor
func BuildDict(samples [][]byte, dictSize int) []byte {
	return DefaultCompressor.BuildDict(samples, dictSize)
}