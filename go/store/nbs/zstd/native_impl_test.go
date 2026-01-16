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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeCompressionRoundtrip(t *testing.T) {
	compressor, err := NewNativeCompressor()
	require.NoError(t, err)

	input := []byte("Hello, world! This is a test of native zstd compression.")

	compressed := compressor.Compress(nil, input)

	decompressed, err := compressor.Decompress(nil, compressed)
	require.NoError(t, err)
	require.Equal(t, string(input), string(decompressed))
}

func TestNativeDictionaryCompressionRoundtrip(t *testing.T) {
	nativeCompressor, err := NewNativeCompressor()
	require.NoError(t, err)

	// Use CGO implementation to build a working dictionary since native BuildDict is unusable
	cgoCompressor := NewGozstdCompressor()

	samples := [][]byte{
		[]byte("This is sample text with common words and phrases."),
		[]byte("Common words and phrases appear frequently."),
		[]byte("Sample text contains common patterns."),
	}

	dictData := cgoCompressor.BuildDict(samples, 256)
	if len(dictData) == 0 {
		t.Skip("Could not build dictionary for testing")
	}

	testData := []byte("This is test data with common words that match the dictionary.")

	// Test that native implementation can use CGO-built dictionary
	cDict, err := nativeCompressor.NewCDict(dictData)
	require.NoError(t, err)

	dDict, err := nativeCompressor.NewDDict(dictData)
	require.NoError(t, err)

	// Compress with dictionary using native implementation
	compressed := nativeCompressor.CompressDict(nil, testData, cDict)

	// Decompress with dictionary using native implementation
	decompressed, err := nativeCompressor.DecompressDict(nil, compressed, dDict)
	require.NoError(t, err)

	// Verify roundtrip
	require.Equal(t, string(testData), string(decompressed))
}
