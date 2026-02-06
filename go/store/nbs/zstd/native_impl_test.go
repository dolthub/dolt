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
	t.Skip("Native implementation cannot build dictionaries, and we can't access CGO implementation in native build")
}

func TestNativeAvailabilityFlag(t *testing.T) {
	require.False(t, IsCGOAvailable, "CGO should not be available in native build")
}
