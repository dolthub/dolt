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

package gitbs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeRange(t *testing.T) {
	start, end, err := NormalizeRange(10, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(10), end)

	start, end, err = NormalizeRange(10, 2, 3)
	require.NoError(t, err)
	require.Equal(t, int64(2), start)
	require.Equal(t, int64(5), end)

	start, end, err = NormalizeRange(10, -3, 0)
	require.NoError(t, err)
	require.Equal(t, int64(7), start)
	require.Equal(t, int64(10), end)

	start, end, err = NormalizeRange(10, -3, 2)
	require.NoError(t, err)
	require.Equal(t, int64(7), start)
	require.Equal(t, int64(9), end)

	_, _, err = NormalizeRange(10, 11, 0)
	require.Error(t, err)

	_, _, err = NormalizeRange(10, -11, 0)
	require.Error(t, err)

	_, _, err = NormalizeRange(10, 0, -1)
	require.Error(t, err)
}

func TestSliceParts(t *testing.T) {
	parts := []PartRef{
		{OIDHex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 3},
		{OIDHex: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Size: 4},
		{OIDHex: "cccccccccccccccccccccccccccccccccccccccc", Size: 2},
	}

	slices, err := SliceParts(parts, 0, 9)
	require.NoError(t, err)
	require.Len(t, slices, 3)
	require.Equal(t, int64(3), slices[0].Length)
	require.Equal(t, int64(4), slices[1].Length)
	require.Equal(t, int64(2), slices[2].Length)

	// Middle slice spanning two parts: [2,5) covers a[2:] + b[:2]
	slices, err = SliceParts(parts, 2, 5)
	require.NoError(t, err)
	require.Equal(t, []PartSlice{
		{OIDHex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Offset: 2, Length: 1},
		{OIDHex: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Offset: 0, Length: 2},
	}, slices)

	// Single-part slice: [3,7) maps to b[0:4]
	slices, err = SliceParts(parts, 3, 7)
	require.NoError(t, err)
	require.Equal(t, []PartSlice{
		{OIDHex: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Offset: 0, Length: 4},
	}, slices)
}
