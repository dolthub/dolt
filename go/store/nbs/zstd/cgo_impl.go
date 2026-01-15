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

import "github.com/dolthub/gozstd"

// GozstdCompressor implements the Compressor interface using github.com/dolthub/gozstd
type GozstdCompressor struct{}

// NewGozstdCompressor creates a new GozstdCompressor
func NewGozstdCompressor() *GozstdCompressor {
	return &GozstdCompressor{}
}

// Compress compresses data using gozstd
func (g *GozstdCompressor) Compress(dst, src []byte) []byte {
	return gozstd.Compress(dst, src)
}

// Decompress decompresses data using gozstd
func (g *GozstdCompressor) Decompress(dst, src []byte) ([]byte, error) {
	return gozstd.Decompress(dst, src)
}

// CompressDict compresses data using a compression dictionary
func (g *GozstdCompressor) CompressDict(dst, src []byte, dict *CDict) []byte {
	if dict == nil || dict.impl == nil {
		panic("runtime error: nil dictionary passed to gozstd compressor")
	}
	if gDict, ok := dict.impl.(*gozstd.CDict); ok {
		return gozstd.CompressDict(dst, src, gDict)
	}
	panic("runtime error: invalid dictionary type for gozstd compressor")
}

// DecompressDict decompresses data using a decompression dictionary
func (g *GozstdCompressor) DecompressDict(dst, src []byte, dict *DDict) ([]byte, error) {
	if dict == nil || dict.impl == nil {
		panic("runtime error: nil dictionary passed to gozstd compressor")
	}
	if gDict, ok := dict.impl.(*gozstd.DDict); ok {
		return gozstd.DecompressDict(dst, src, gDict)
	}
	panic("runtime error: invalid dictionary type for gozstd compressor")
}

// NewCDict creates a new compression dictionary
func (g *GozstdCompressor) NewCDict(dict []byte) (*CDict, error) {
	cDict, err := gozstd.NewCDict(dict)
	if err != nil {
		return nil, err
	}
	return &CDict{impl: cDict}, nil
}

// NewDDict creates a new decompression dictionary
func (g *GozstdCompressor) NewDDict(dict []byte) (*DDict, error) {
	dDict, err := gozstd.NewDDict(dict)
	if err != nil {
		return nil, err
	}
	return &DDict{impl: dDict}, nil
}

// BuildDict builds a dictionary from training samples
func (g *GozstdCompressor) BuildDict(samples [][]byte, dictSize int) []byte {
	return gozstd.BuildDict(samples, dictSize)
}
