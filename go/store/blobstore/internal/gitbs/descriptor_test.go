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

func TestEncodeParseDescriptor_RoundTrip(t *testing.T) {
	d := Descriptor{
		TotalSize: 7,
		Parts: []PartRef{
			{OIDHex: "0123456789abcdef0123456789abcdef01234567", Size: 3},
			{OIDHex: "89abcdef0123456789abcdef0123456789abcdef", Size: 4},
		},
	}

	b, err := EncodeDescriptor(d)
	require.NoError(t, err)

	got, err := ParseDescriptor(b)
	require.NoError(t, err)
	require.Equal(t, d, got)
}

func TestParseDescriptor_InvalidMagic(t *testing.T) {
	_, err := ParseDescriptor([]byte("NOPE\nsize 0\n"))
	require.Error(t, err)
}

func TestParseDescriptor_MissingSizeLine(t *testing.T) {
	_, err := ParseDescriptor([]byte("DOLTBS1\npart 0123456789abcdef0123456789abcdef01234567 1\n"))
	require.Error(t, err)
}

func TestParseDescriptor_MultipleSizeLines(t *testing.T) {
	_, err := ParseDescriptor([]byte("DOLTBS1\nsize 1\nsize 2\n"))
	require.Error(t, err)
}

func TestParseDescriptor_UnknownLine(t *testing.T) {
	_, err := ParseDescriptor([]byte("DOLTBS1\nsize 0\nwat 1\n"))
	require.Error(t, err)
}

func TestParseDescriptor_InvalidOID(t *testing.T) {
	_, err := ParseDescriptor([]byte("DOLTBS1\nsize 1\npart not-an-oid 1\n"))
	require.Error(t, err)
}

func TestParseDescriptor_PartSizeZeroRejected(t *testing.T) {
	_, err := ParseDescriptor([]byte("DOLTBS1\nsize 0\npart 0123456789abcdef0123456789abcdef01234567 0\n"))
	require.Error(t, err)
}

func TestParseDescriptor_SumMismatch(t *testing.T) {
	_, err := ParseDescriptor([]byte("DOLTBS1\nsize 2\npart 0123456789abcdef0123456789abcdef01234567 1\n"))
	require.Error(t, err)
}

func TestParseDescriptor_TotalSizeZeroRequiresNoParts(t *testing.T) {
	_, err := ParseDescriptor([]byte("DOLTBS1\nsize 0\npart 0123456789abcdef0123456789abcdef01234567 1\n"))
	require.Error(t, err)
}

func TestEncodeDescriptor_Validates(t *testing.T) {
	_, err := EncodeDescriptor(Descriptor{TotalSize: 1})
	require.Error(t, err)
}

func TestIsDescriptorPrefix(t *testing.T) {
	require.True(t, IsDescriptorPrefix([]byte("DOLTBS1\nsize ")))
	require.True(t, IsDescriptorPrefix([]byte("DOLTBS1\r\nsize ")))
	require.False(t, IsDescriptorPrefix([]byte("DOLTBS")))
	require.False(t, IsDescriptorPrefix([]byte("xxxxDOLTBS1\n")))
	require.False(t, IsDescriptorPrefix([]byte("DOLTBS1\nthis is not a descriptor\n")))
}
