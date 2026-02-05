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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangesHelpers_normalizeStartEnd(t *testing.T) {
	start, err := normalizeStart(10, -2)
	require.NoError(t, err)
	require.Equal(t, int64(8), start)

	_, err = normalizeStart(10, 11)
	require.Error(t, err)

	end, err := normalizeEnd(10, 2, 0)
	require.NoError(t, err)
	require.Equal(t, int64(10), end)

	end, err = normalizeEnd(10, 2, 100)
	require.NoError(t, err)
	require.Equal(t, int64(10), end)
}

func TestRangesHelpers_partBoundsAndOverlap(t *testing.T) {
	_, _, err := partBounds(0, 0)
	require.Error(t, err)

	// Force int64 overflow path: end wraps negative, so end < start.
	_, _, err = partBounds(math.MaxInt64-1, 10)
	require.Error(t, err)

	s, e, ok := overlap(0, 10, 2, 5)
	require.True(t, ok)
	require.Equal(t, int64(2), s)
	require.Equal(t, int64(5), e)

	_, _, ok = overlap(0, 10, 10, 12)
	require.False(t, ok)
}

func TestRangesHelpers_validateCoverage(t *testing.T) {
	_, err := validateCoverage(nil, 0, 1)
	require.Error(t, err)

	_, err = validateCoverage([]PartSlice{{OIDHex: "a", Offset: 0, Length: 1}}, 0, 2)
	require.Error(t, err)

	out, err := validateCoverage([]PartSlice{{OIDHex: "a", Offset: 0, Length: 2}}, 0, 2)
	require.NoError(t, err)
	require.Len(t, out, 1)
}
