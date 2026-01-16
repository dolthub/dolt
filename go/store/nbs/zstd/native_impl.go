//go:build zstd_native

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

	nativezstd "github.com/klauspost/compress/zstd"
)

func createDefaultCompressor() Compressor {
	nativeCompressor, err := NewNativeCompressor()
	if err != nil {
		panic("failed to initialize native zstd compressor: " + err.Error())
	}
	return nativeCompressor
}

// nativeDictEncoder wraps a zstd encoder configured with a dictionary
type nativeDictEncoder struct {
	encoder *nativezstd.Encoder
}

// nativeDictDecoder wraps a zstd decoder configured with a dictionary
type nativeDictDecoder struct {
	decoder *nativezstd.Decoder
}

// NativeCompressor implements the Compressor interface using github.com/klauspost/compress/zstd
type NativeCompressor struct {
	encoder *nativezstd.Encoder
	decoder *nativezstd.Decoder
}

// NewNativeCompressor creates a new NativeCompressor
func NewNativeCompressor() (*NativeCompressor, error) {
	encoder, err := nativezstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}

	decoder, err := nativezstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	return &NativeCompressor{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

// Compress compresses data using klauspost/compress/zstd
func (n *NativeCompressor) Compress(dst, src []byte) []byte {
	return n.encoder.EncodeAll(src, dst)
}

// Decompress decompresses data using klauspost/compress/zstd
func (n *NativeCompressor) Decompress(dst, src []byte) ([]byte, error) {
	return n.decoder.DecodeAll(src, dst)
}

// CompressDict compresses data using a compression dictionary
func (n *NativeCompressor) CompressDict(dst, src []byte, dict *CDict) ([]byte, error) {
	if dict == nil || dict.impl == nil {
		return nil, fmt.Errorf("nil dictionary passed to native compressor")
	}
	if dictEncoder, ok := dict.impl.(*nativeDictEncoder); ok {
		return dictEncoder.encoder.EncodeAll(src, dst), nil
	}
	return nil, fmt.Errorf("invalid dictionary type for native compressor")
}

// DecompressDict decompresses data using a decompression dictionary
func (n *NativeCompressor) DecompressDict(dst, src []byte, dict *DDict) ([]byte, error) {
	if dict == nil || dict.impl == nil {
		return nil, fmt.Errorf("nil dictionary passed to native compressor")
	}
	if dictDecoder, ok := dict.impl.(*nativeDictDecoder); ok {
		return dictDecoder.decoder.DecodeAll(src, dst)
	}
	return nil, fmt.Errorf("invalid dictionary type for native compressor")
}

// NewCDict creates a new compression dictionary
func (n *NativeCompressor) NewCDict(dict []byte) (*CDict, error) {
	encoder, err := nativezstd.NewWriter(nil, nativezstd.WithEncoderDict(dict))
	if err != nil {
		return nil, err
	}
	return &CDict{impl: &nativeDictEncoder{encoder: encoder}}, nil
}

// NewDDict creates a new decompression dictionary
func (n *NativeCompressor) NewDDict(dict []byte) (*DDict, error) {
	decoder, err := nativezstd.NewReader(nil, nativezstd.WithDecoderDicts(dict))
	if err != nil {
		return nil, err
	}
	return &DDict{impl: &nativeDictDecoder{decoder: decoder}}, nil
}

// BuildDict always return the static dictionary and ignores the samples provided. This allocates a copy
// of the static dictionary each time it is called.
func (n *NativeCompressor) BuildDict(_ [][]byte, _ int) []byte {
	b := make([]byte, len(staticDictionary))
	copy(b, staticDictionary)
	return b
}

var staticDictionary = []byte{
	0x37, 0x28, 0xB6, 0xCA, 0xAF, 0x2D, 0xE5, 0xA3,
	0xF3, 0xC5, 0xD4, 0x5B, 0xC1, 0x6E, 0x8E, 0x9E,
}
