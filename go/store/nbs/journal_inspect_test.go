// Copyright 2025 Dolthub, Inc.
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

package nbs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanMysteryBytes_SingleByteTwoCrc(t *testing.T) {
	buf := []byte{0x45, 0x26, 0xf7, 0x5a, 0xf1, 0x6b, 0xc5, 0xf6, 0xb6}
	//  Manually verified CRC values:
	//	crcVal := crc(buf[0:1])
	//	require.Equal(t, crcVal, uint32(0x26f75af1))
	//	crcVal = crc(buf[0:5])
	//	require.Equal(t, crcVal, uint32(0x6bc5f6b6))

	matches := scanMysteryBytesForCRCs(0, buf)
	require.Equal(t, 1, len(matches))
	require.Equal(t, uint32(0), matches[0].start)
	require.Equal(t, uint32(9), matches[0].end)
	require.Equal(t, 1, len(matches[0].nested))
	require.Equal(t, uint32(0), matches[0].nested[0].start)
	require.Equal(t, uint32(5), matches[0].nested[0].end)
	require.Equal(t, 0, len(matches[0].nested[0].nested))
}

func TestScanMysteryBytes_PaddedSingleByteTwoCRC(t *testing.T) {
	// Same as the previous test, but with leading and trailing garbage bytes
	buf := []byte{0xaa, 0xbb, 0xcc, 0x45, 0x26, 0xf7, 0x5a, 0xf1, 0x6b, 0xc5, 0xf6, 0xb6, 0xdd, 0xee}

	matches := scanMysteryBytesForCRCs(0, buf)
	require.Equal(t, 1, len(matches))
	require.Equal(t, uint32(3), matches[0].start)
	require.Equal(t, uint32(12), matches[0].end)
	require.Equal(t, 1, len(matches[0].nested))
	require.Equal(t, uint32(3), matches[0].nested[0].start)
	require.Equal(t, uint32(8), matches[0].nested[0].end)
	require.Equal(t, 0, len(matches[0].nested[0].nested))
}

func TestScanMysteryBytes_MultipleResults(t *testing.T) {
	// Single byte CRC concatenated with garbage inbetween
	buf := []byte{0x45, 0x26, 0xf7, 0x5a, 0xf1, 0xff, 0x00, 0xf0, 0x0f, 0x45, 0x26, 0xf7, 0x5a, 0xf1}

	matches := scanMysteryBytesForCRCs(0, buf)
	require.Equal(t, 2, len(matches))
	require.Equal(t, uint32(0), matches[0].start)
	require.Equal(t, uint32(5), matches[0].end)
	require.Equal(t, 0, len(matches[0].nested))
	require.Equal(t, uint32(9), matches[1].start)
	require.Equal(t, uint32(14), matches[1].end)
	require.Equal(t, 0, len(matches[1].nested))
}

func TestScanMysteryBytes_ManyNestedResults(t *testing.T) {
	// We never plan to have more than 2 levels of nesting in real journal records, so this doesn't represent something
	// we expect to see, but heh this is all stuff we don't expect.
	buf := []byte{0xFF, 0x45, 0x26, 0xf7, 0x5a, 0xf1, 0x6b, 0xc5, 0xf6, 0xb6, 0x6b, 0xbc, 0xc5, 0x00}
	//  Manually verified CRC values:
	// crcVal := crc(buf[1:len(buf)-4])
	// require.Equal(t, crcVal, uint32(0x6bbcc500))
	matches := scanMysteryBytesForCRCs(0, buf)
	require.Equal(t, 1, len(matches))
	require.Equal(t, uint32(1), matches[0].start)
	require.Equal(t, uint32(14), matches[0].end)
	require.Equal(t, 1, len(matches[0].nested))
	require.Equal(t, uint32(1), matches[0].nested[0].start)
	require.Equal(t, uint32(10), matches[0].nested[0].end)
	require.Equal(t, 1, len(matches[0].nested[0].nested))
	require.Equal(t, uint32(1), matches[0].nested[0].nested[0].start)
	require.Equal(t, uint32(6), matches[0].nested[0].nested[0].end)
	require.Equal(t, 0, len(matches[0].nested[0].nested[0].nested))
}

func TestScanMysteryBytes_NoMatch(t *testing.T) {
	buf := []byte("random bytes without crc")
	matches := scanMysteryBytesForCRCs(0, buf)
	require.Empty(t, matches)
}

func TestScanMysteryBytes_ShortBuffers(t *testing.T) {
	require.Empty(t, scanMysteryBytesForCRCs(0, []byte{}))
	require.Empty(t, scanMysteryBytesForCRCs(0, []byte{1, 2, 3, 4}))
}
