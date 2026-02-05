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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescriptorHelpers_validateDescriptorParts(t *testing.T) {
	sum, err := validateDescriptorParts([]PartRef{
		{OIDHex: "0123456789abcdef0123456789abcdef01234567", Size: 3},
		{OIDHex: "89abcdef0123456789abcdef0123456789abcdef", Size: 4},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), sum)

	_, err = validateDescriptorParts([]PartRef{{OIDHex: "not-an-oid", Size: 1}})
	require.Error(t, err)

	_, err = validateDescriptorParts([]PartRef{{OIDHex: "0123456789abcdef0123456789abcdef01234567", Size: 0}})
	require.Error(t, err)
}

func TestDescriptorHelpers_validateDescriptorSizeAndParts(t *testing.T) {
	require.NoError(t, validateDescriptorSizeAndParts(0, 0, 0))
	require.Error(t, validateDescriptorSizeAndParts(0, 1, 1))
	require.Error(t, validateDescriptorSizeAndParts(1, 0, 0))
	require.Error(t, validateDescriptorSizeAndParts(3, 1, 2))
	require.NoError(t, validateDescriptorSizeAndParts(3, 1, 3))
}

func TestDescriptorHelpers_parseLines(t *testing.T) {
	var st descriptorParseState

	err := parseDescriptorLine(&st, "size 3")
	require.NoError(t, err)
	require.True(t, st.haveSz)
	require.Equal(t, uint64(3), st.d.TotalSize)

	err = parseDescriptorLine(&st, "part 0123456789abcdef0123456789abcdef01234567 3")
	require.NoError(t, err)
	require.Len(t, st.d.Parts, 1)

	d, err := finalizeParsedDescriptor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(3), d.TotalSize)
	require.Len(t, d.Parts, 1)
}

func TestDescriptorHelpers_writePartLine(t *testing.T) {
	var b strings.Builder
	writePartLine(&b, PartRef{OIDHex: "0123456789abcdef0123456789abcdef01234567", Size: 9})
	require.Equal(t, "part 0123456789abcdef0123456789abcdef01234567 9\n", b.String())
}

