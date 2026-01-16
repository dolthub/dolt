//go:build !zstd_native

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

import (
	"fmt"

	cgozstd "github.com/dolthub/gozstd"
)

const BuildDictEnabled = true

func createDefaultCompressor() Compressor {
	return NewGozstdCompressor()
}

type CGOzstdCompressor struct{}

func NewGozstdCompressor() *CGOzstdCompressor {
	return &CGOzstdCompressor{}
}

func (g *CGOzstdCompressor) Compress(dst, src []byte) []byte {
	return cgozstd.Compress(dst, src)
}

func (g *CGOzstdCompressor) Decompress(dst, src []byte) ([]byte, error) {
	return cgozstd.Decompress(dst, src)
}

func (g *CGOzstdCompressor) CompressDict(dst, src []byte, dict *CDict) ([]byte, error) {
	if dict == nil || dict.impl == nil {
		return nil, fmt.Errorf("nil dictionary passed to gozstd compressor")
	}
	if gDict, ok := dict.impl.(*cgozstd.CDict); ok {
		return cgozstd.CompressDict(dst, src, gDict), nil
	}
	return nil, fmt.Errorf("invalid dictionary type for gozstd compressor")
}

func (g *CGOzstdCompressor) DecompressDict(dst, src []byte, dict *DDict) ([]byte, error) {
	if dict == nil || dict.impl == nil {
		return nil, fmt.Errorf("nil dictionary passed to gozstd compressor")
	}
	if gDict, ok := dict.impl.(*cgozstd.DDict); ok {
		return cgozstd.DecompressDict(dst, src, gDict)
	}
	return nil, fmt.Errorf("invalid dictionary type for gozstd compressor")
}

func (g *CGOzstdCompressor) NewCDict(dict []byte) (*CDict, error) {
	cDict, err := cgozstd.NewCDict(dict)
	if err != nil {
		return nil, err
	}
	return &CDict{impl: cDict}, nil
}

func (g *CGOzstdCompressor) NewDDict(dict []byte) (*DDict, error) {
	dDict, err := cgozstd.NewDDict(dict)
	if err != nil {
		return nil, err
	}
	return &DDict{impl: dDict}, nil
}

func (g *CGOzstdCompressor) BuildDict(samples [][]byte, dictSize int) []byte {
	return cgozstd.BuildDict(samples, dictSize)
}
