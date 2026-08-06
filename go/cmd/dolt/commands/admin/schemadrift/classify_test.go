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

package schemadrift

import (
	"bytes"
	"context"
	"testing"

	"github.com/mohae/uvarint"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// fakeChunkPresence implements ChunkPresenceChecker for tests, with explicit
// per-hash "present" answers.
type fakeChunkPresence struct{ present map[hash.Hash]bool }

func (f *fakeChunkPresence) Has(ctx context.Context, h hash.Hash) (bool, error) {
	return f.present[h], nil
}

// TestClassifyFieldBytes_NULL covers the empty-slice case, which is what
// val.Tuple.GetField returns for a NULL field.
func TestClassifyFieldBytes_NULL(t *testing.T) {
	require.Equal(t, FieldNULL, ClassifyFieldBytes(nil))
	require.Equal(t, FieldNULL, ClassifyFieldBytes([]byte{}))
}

// TestClassifyFieldBytes_AdaptiveInline covers the genuine adaptive-inline
// case: [0x00][inline bytes...]. The inline payload can be anything.
func TestClassifyFieldBytes_AdaptiveInline(t *testing.T) {
	cases := [][]byte{
		{0x00},           // empty inline string
		{0x00, 'h', 'i'}, // short inline
		append([]byte{0x00}, bytes.Repeat([]byte{'x'}, 100)...), // 100-byte inline
		append([]byte{0x00}, bytes.Repeat([]byte{0xff}, 19)...), // 20-byte total with 0x00 leader — this is adaptive inline, NOT legacy
	}
	for _, b := range cases {
		require.Equal(t, FieldAdaptiveInline, ClassifyFieldBytes(b), "bytes=% x", b)
	}
}

// TestClassifyFieldBytes_LegacyRawHash covers the smoking-gun case: a 1.x
// StringAddrEnc / BytesAddrEnc / JSONAddrEnc / GeomAddrEnc field is exactly
// 20 bytes of raw hash with a non-zero leading byte. This is what the drift
// detector keys on.
func TestClassifyFieldBytes_LegacyRawHash(t *testing.T) {
	// Build a 20-byte hash with a non-zero leading byte.
	var raw [hash.ByteLen]byte
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	require.Equal(t, byte(1), raw[0])
	require.Equal(t, FieldLegacyRawHash, ClassifyFieldBytes(raw[:]))

	// Sanity: 20 bytes leading with 0x00 must NOT classify as legacy — that
	// would be ambiguous with adaptive inline, and we deliberately route that
	// to FieldAdaptiveInline.
	zeroLed := make([]byte, hash.ByteLen)
	zeroLed[0] = 0x00
	require.Equal(t, FieldAdaptiveInline, ClassifyFieldBytes(zeroLed))
}

// TestClassifyFieldBytes_AdaptiveAddressed exercises the well-formed adaptive
// addressed shape: varint(declared length) + hash.ByteLen. The varint can be
// 1-9 bytes long.
func TestClassifyFieldBytes_AdaptiveAddressed(t *testing.T) {
	// 1-byte varint encoding a small length, then a 20-byte hash.
	short := encodeAdaptiveAddressed(t, 42, byte(1))
	require.Equal(t, hash.ByteLen+1, len(short))
	require.Equal(t, FieldAdaptiveAddressed, ClassifyFieldBytes(short))

	// Multi-byte varint encoding a larger length.
	big := encodeAdaptiveAddressed(t, 1<<20 /*=1MiB*/, byte(2))
	require.Equal(t, FieldAdaptiveAddressed, ClassifyFieldBytes(big),
		"adaptive-addressed value with multi-byte varint must classify correctly")
}

// TestClassifyFieldBytes_Unknown covers shapes we deliberately refuse to
// auto-classify: anything outside the four valid layouts.
func TestClassifyFieldBytes_Unknown(t *testing.T) {
	// Non-zero leader, 19 bytes total — too short for both legacy (20)
	// and adaptive addressed (21+).
	require.Equal(t, FieldUnknown, ClassifyFieldBytes(bytes.Repeat([]byte{0x01}, 19)))

	// Non-zero leader, 21+ bytes but varint length declares a trailer that
	// doesn't match. Construct: [0x05] [10 zero bytes] — leader 0x05 is a
	// single-byte varint (value 5), so the parser would expect total length =
	// 1 + 20 = 21, but we give it 11 bytes. Should be FieldUnknown.
	bad := append([]byte{0x05}, bytes.Repeat([]byte{0x00}, 10)...)
	require.Equal(t, FieldUnknown, ClassifyFieldBytes(bad))
}

// TestIsLegacyEncoding_pairs and TestIsAdaptiveEncoding_pairs are the unit
// guards that LegacySibling is symmetric on the encoding pairs we support.
// LegacySibling is what `repair` consults to derive the new schema encoding;
// any drift here would silently mis-flip the persisted tag.
func TestLegacySibling_pairs(t *testing.T) {
	pairs := []struct {
		adaptive val.Encoding
		legacy   val.Encoding
	}{
		{val.StringAdaptiveEnc, val.StringAddrEnc},
		{val.BytesAdaptiveEnc, val.BytesAddrEnc},
		{val.JsonAdaptiveEnc, val.JSONAddrEnc},
		{val.GeomAdaptiveEnc, val.GeomAddrEnc},
		{val.ExtendedAdaptiveEnc, val.ExtendedAddrEnc},
	}
	for _, p := range pairs {
		got, ok := LegacySibling(p.adaptive)
		require.True(t, ok, "LegacySibling(%v) must succeed", p.adaptive)
		require.Equal(t, p.legacy, got)

		require.True(t, IsAdaptiveEncoding(p.adaptive), "%v must be classified adaptive", p.adaptive)
		require.True(t, IsLegacyEncoding(p.legacy), "%v must be classified legacy", p.legacy)

		require.False(t, IsLegacyEncoding(p.adaptive))
		require.False(t, IsAdaptiveEncoding(p.legacy))
	}

	// Non-pair fixed-encoding inputs must NOT have a legacy sibling.
	_, ok := LegacySibling(val.StringAddrEnc)
	require.False(t, ok, "legacy encoding should not have its own legacy sibling")
	_, ok = LegacySibling(val.Encoding(0))
	require.False(t, ok, "unknown encoding should not have a legacy sibling")
}

// TestClassifyFieldWithChunkstore_AmbiguousZeroLeadingPromotesOnHashHit is
// the load-bearing test for the chunkstore-aware classifier — the case the
// structural classifier alone cannot solve. A 20-byte field starting with
// 0x00 is structurally an adaptive-inline value with 19 inline bytes; the
// chunkstore lookup is the only way to tell whether it's actually a legacy
// raw hash whose first byte happens to be 0x00 (~1/256 of all hashes).
//
// When the chunkstore reports that hash.New(field) IS present, promote to
// FieldLegacyRawHash. When it is NOT present, trust the structural
// FieldAdaptiveInline classification.
func TestClassifyFieldWithChunkstore_AmbiguousZeroLeadingPromotesOnHashHit(t *testing.T) {
	ctx := context.Background()

	// Build a 20-byte field with leading 0x00 — the structurally ambiguous
	// shape.
	var raw [hash.ByteLen]byte
	raw[0] = 0x00
	for i := 1; i < hash.ByteLen; i++ {
		raw[i] = byte(i)
	}
	field := raw[:]
	h := hash.New(field)

	// Case A: chunkstore HAS this hash → field is a legacy raw hash whose
	// first byte coincidentally happens to be 0x00. Must promote.
	csHit := &fakeChunkPresence{present: map[hash.Hash]bool{h: true}}
	f, err := ClassifyFieldWithChunkstore(ctx, csHit, field)
	require.NoError(t, err)
	require.Equal(t, FieldLegacyRawHash, f,
		"a 20-byte 0x00-leading field whose hash IS present in the chunkstore must classify as legacy raw hash")

	// Case B: chunkstore does NOT have this hash → field is a genuine
	// adaptive-inline value with 19 zero-content inline bytes (or whatever
	// inline content). Stay at FieldAdaptiveInline.
	csMiss := &fakeChunkPresence{present: map[hash.Hash]bool{}}
	f, err = ClassifyFieldWithChunkstore(ctx, csMiss, field)
	require.NoError(t, err)
	require.Equal(t, FieldAdaptiveInline, f,
		"a 20-byte 0x00-leading field whose hash is NOT present must classify as genuine adaptive inline")

	// Case C: no chunkstore (nil) → trust structural classification.
	f, err = ClassifyFieldWithChunkstore(ctx, nil, field)
	require.NoError(t, err)
	require.Equal(t, FieldAdaptiveInline, f,
		"with no chunkstore, ambiguous 20-byte 0x00-leading must default to the structural answer")
}

// TestClassifyFieldWithChunkstore_NoAmbiguityShortcuts confirms the chunkstore
// is not consulted for unambiguous shapes. We use a "never called" fake to
// verify it.
func TestClassifyFieldWithChunkstore_NoAmbiguityShortcuts(t *testing.T) {
	ctx := context.Background()

	// 20 bytes, leading 0x01 — unambiguously legacy.
	legacy := make([]byte, hash.ByteLen)
	legacy[0] = 0x01

	cs := &neverCalledChunkPresence{t: t}
	f, err := ClassifyFieldWithChunkstore(ctx, cs, legacy)
	require.NoError(t, err)
	require.Equal(t, FieldLegacyRawHash, f)

	// 21-byte adaptive addressed — unambiguous.
	addressed := encodeAdaptiveAddressed(t, 99, byte(7))
	f, err = ClassifyFieldWithChunkstore(ctx, cs, addressed)
	require.NoError(t, err)
	require.Equal(t, FieldAdaptiveAddressed, f)

	// 5-byte 0x00-leading inline — too short to be ambiguous.
	shortInline := []byte{0x00, 'h', 'i', '!', '!'}
	f, err = ClassifyFieldWithChunkstore(ctx, cs, shortInline)
	require.NoError(t, err)
	require.Equal(t, FieldAdaptiveInline, f)
}

type neverCalledChunkPresence struct{ t *testing.T }

func (n *neverCalledChunkPresence) Has(ctx context.Context, h hash.Hash) (bool, error) {
	n.t.Fatalf("chunkstore must not be consulted for unambiguous classifications, but Has was called on %s", h)
	return false, nil
}

// encodeAdaptiveAddressed constructs a synthetic adaptive-addressed value
// with the given declared length and a deterministic hash whose leading byte
// is |hashLeader|. Used by the classifier tests to keep them independent of
// any real chunkstore.
func encodeAdaptiveAddressed(t *testing.T, length uint64, hashLeader byte) []byte {
	t.Helper()
	buf := make([]byte, 9) // max varint length
	n := uvarint.Encode(buf, length)
	out := append([]byte{}, buf[:n]...)
	var h [hash.ByteLen]byte
	h[0] = hashLeader
	for i := 1; i < hash.ByteLen; i++ {
		h[i] = byte(i)
	}
	return append(out, h[:]...)
}
