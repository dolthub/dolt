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

import (
	"github.com/klauspost/compress/zstd"
)

// nativeDictEncoder wraps a zstd encoder configured with a dictionary
type nativeDictEncoder struct {
	encoder *zstd.Encoder
}

// nativeDictDecoder wraps a zstd decoder configured with a dictionary
type nativeDictDecoder struct {
	decoder *zstd.Decoder
}

// NativeCompressor implements the Compressor interface using github.com/klauspost/compress/zstd
type NativeCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewNativeCompressor creates a new NativeCompressor
func NewNativeCompressor() (*NativeCompressor, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(nil)
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
func (n *NativeCompressor) CompressDict(dst, src []byte, dict *CDict) []byte {
	if dict == nil || dict.impl == nil {
		panic("runtime error: nil dictionary passed to native compressor")
	}
	if dictEncoder, ok := dict.impl.(*nativeDictEncoder); ok {
		return dictEncoder.encoder.EncodeAll(src, dst)
	}
	panic("runtime error: invalid dictionary type for native compressor")
}

// DecompressDict decompresses data using a decompression dictionary
func (n *NativeCompressor) DecompressDict(dst, src []byte, dict *DDict) ([]byte, error) {
	if dict == nil || dict.impl == nil {
		panic("runtime error: nil dictionary passed to native compressor")
	}
	if dictDecoder, ok := dict.impl.(*nativeDictDecoder); ok {
		return dictDecoder.decoder.DecodeAll(src, dst)
	}
	panic("runtime error: invalid dictionary type for native compressor")
}

// NewCDict creates a new compression dictionary
func (n *NativeCompressor) NewCDict(dict []byte) (*CDict, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict))
	if err != nil {
		return nil, err
	}
	return &CDict{impl: &nativeDictEncoder{encoder: encoder}}, nil
}

// NewDDict creates a new decompression dictionary
func (n *NativeCompressor) NewDDict(dict []byte) (*DDict, error) {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return nil, err
	}
	return &DDict{impl: &nativeDictDecoder{decoder: decoder}}, nil
}

// BuildDict builds a dictionary from training samples
func (n *NativeCompressor) BuildDict(samples [][]byte, dictSize int) []byte {
	// klauspost/compress/zstd BuildDict is effectively unusable - it fails even with
	// thousands of samples with obvious repeated patterns. Build logic should prevent us from reaching
	// this point.
	panic("runtime error: BuildDict not supported in native implementation - use CGO implementation")
}
