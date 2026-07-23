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
	"errors"
	"testing"

	"github.com/mohae/uvarint"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

// stubValueStore is a minimal val.ValueStore implementation for unit tests on
// resolveFieldToLegacy. It records every Write and supplies a deterministic
// hash so tests can assert what got written.
type stubValueStore struct {
	writes [][]byte
	reads  map[hash.Hash][]byte
	// readErr is returned by ReadBytes if non-nil. Lets us simulate a missing
	// NBS chunk without constructing one.
	readErr error
	// writeErr is returned by WriteBytes if non-nil. Simulates a chunkstore
	// write failure.
	writeErr error
}

func newStubVS() *stubValueStore {
	return &stubValueStore{reads: make(map[hash.Hash][]byte)}
}

func (s *stubValueStore) ReadBytes(ctx context.Context, h hash.Hash) ([]byte, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	if b, ok := s.reads[h]; ok {
		return b, nil
	}
	return nil, errors.New("stubValueStore: hash not present")
}

func (s *stubValueStore) WriteBytes(ctx context.Context, b []byte) (hash.Hash, error) {
	if s.writeErr != nil {
		return hash.Hash{}, s.writeErr
	}
	s.writes = append(s.writes, append([]byte(nil), b...))
	// Deterministic: synthesize a hash from a sequence number derived from
	// the content + the write count so duplicates collide as they would in
	// a real content-addressed store.
	h := hash.Of(b)
	s.reads[h] = append([]byte(nil), b...)
	return h, nil
}

// stubChunkPresence is the test counterpart to ChunkPresenceChecker. Per-hash
// truth values are supplied at construction.
type stubChunkPresence struct {
	present map[hash.Hash]bool
	hasErr  error
}

func (s *stubChunkPresence) Has(ctx context.Context, h hash.Hash) (bool, error) {
	if s.hasErr != nil {
		return false, s.hasErr
	}
	return s.present[h], nil
}

// TestResolveFieldToLegacy_NULL covers the NULL (empty slice) case. NULL is
// represented identically in legacy and adaptive, so the result is unchanged.
func TestResolveFieldToLegacy_NULL(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	out, isLegacy, changed, err := resolveFieldToLegacy(ctx, nil, vs, cs)
	require.NoError(t, err)
	require.True(t, isLegacy, "NULL is canonical legacy")
	require.False(t, changed, "NULL bytes must not change")
	require.Nil(t, out)

	out, isLegacy, changed, err = resolveFieldToLegacy(ctx, []byte{}, vs, cs)
	require.NoError(t, err)
	require.True(t, isLegacy)
	require.False(t, changed)
	require.Nil(t, out)

	require.Empty(t, vs.writes, "NULL must not write to chunkstore")
}

// TestResolveFieldToLegacy_LegacyRawHash covers the unambiguously-legacy
// shape: 20 bytes with a non-zero leading byte. Already canonical, no rewrite.
func TestResolveFieldToLegacy_LegacyRawHash(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	// Build a 20-byte field with a non-zero leading byte.
	var raw [hash.ByteLen]byte
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	require.NotEqual(t, byte(0), raw[0])

	out, isLegacy, changed, err := resolveFieldToLegacy(ctx, raw[:], vs, cs)
	require.NoError(t, err)
	require.True(t, isLegacy, "legacy raw hash is canonical")
	require.False(t, changed, "legacy raw hash bytes must not change")
	require.Equal(t, raw[:], out, "output bytes must be the input bytes verbatim")
	require.Empty(t, vs.writes, "legacy raw hash must not write to chunkstore")
}

// TestResolveFieldToLegacy_AdaptiveAddressed covers the adaptive addressed
// shape: varint(length) + 20-byte hash. The migration extracts the trailing
// 20 bytes and returns them as the new field; no content is re-written.
func TestResolveFieldToLegacy_AdaptiveAddressed(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	// Build a synthetic adaptive-addressed value: 1-byte varint = 42, then 20
	// hash bytes.
	buf := make([]byte, 9)
	n := uvarint.Encode(buf, 42)
	field := append([]byte{}, buf[:n]...)
	var h [hash.ByteLen]byte
	for i := range h {
		h[i] = byte(i + 100) // distinct from legacy test
	}
	field = append(field, h[:]...)
	require.Equal(t, n+hash.ByteLen, len(field))

	out, isLegacy, changed, err := resolveFieldToLegacy(ctx, field, vs, cs)
	require.NoError(t, err)
	require.False(t, isLegacy, "adaptive-addressed is not legacy")
	require.True(t, changed, "adaptive-addressed must rewrite to legacy shape")
	require.Equal(t, hash.ByteLen, len(out), "rewritten field is exactly 20 bytes")
	require.Equal(t, h[:], out, "rewritten bytes are the trailing 20 from the input")
	require.Empty(t, vs.writes, "adaptive-addressed must NOT write to chunkstore — content is already OOB")

	// Multi-byte varint case.
	buf = make([]byte, 9)
	n = uvarint.Encode(buf, 1<<20)
	field2 := append([]byte{}, buf[:n]...)
	field2 = append(field2, h[:]...)
	out, isLegacy, changed, err = resolveFieldToLegacy(ctx, field2, vs, cs)
	require.NoError(t, err)
	require.False(t, isLegacy)
	require.True(t, changed)
	require.Equal(t, h[:], out)
	require.Empty(t, vs.writes)
}

// TestResolveFieldToLegacy_AdaptiveInline covers the inline shape: leading
// 0x00 byte + inline content. The migration writes the inline content to the
// chunkstore and stores the resulting hash as the new field bytes.
func TestResolveFieldToLegacy_AdaptiveInline(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	// Short inline content: leader + "hello"
	field := append([]byte{0x00}, []byte("hello")...)

	out, isLegacy, changed, err := resolveFieldToLegacy(ctx, field, vs, cs)
	require.NoError(t, err)
	require.False(t, isLegacy, "adaptive inline is not legacy")
	require.True(t, changed, "adaptive inline must rewrite to legacy shape")
	require.Equal(t, hash.ByteLen, len(out))
	require.Len(t, vs.writes, 1, "inline content must be written to chunkstore exactly once")
	require.Equal(t, []byte("hello"), vs.writes[0], "chunkstore must receive the inline content (minus the leader byte)")

	// The new field bytes must equal hash.Of("hello") so a subsequent
	// vs.ReadBytes round-trips.
	expectedHash := hash.Of([]byte("hello"))
	require.Equal(t, expectedHash[:], out)
}

// TestResolveFieldToLegacy_ZeroLeading20Bytes_ChunkstoreHit covers the
// load-bearing disambiguation: a 20-byte field whose leading byte is 0x00 is
// structurally ambiguous between (a) legacy raw hash with coincidental 0x00
// leader, and (b) adaptive inline with 19 bytes of content. The chunkstore
// presence check resolves it: a HIT promotes to legacy raw hash (canonical,
// no rewrite); a MISS treats it as inline and rewrites.
func TestResolveFieldToLegacy_ZeroLeading20Bytes_ChunkstoreHit(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()

	var raw [hash.ByteLen]byte
	raw[0] = 0x00
	for i := 1; i < hash.ByteLen; i++ {
		raw[i] = byte(i)
	}
	h := hash.New(raw[:])

	cs := &stubChunkPresence{present: map[hash.Hash]bool{h: true}}

	out, isLegacy, changed, err := resolveFieldToLegacy(ctx, raw[:], vs, cs)
	require.NoError(t, err)
	require.True(t, isLegacy, "chunkstore hit promotes 0x00-leading 20-byte to canonical legacy")
	require.False(t, changed, "chunkstore-hit case must not change bytes")
	require.Equal(t, raw[:], out)
	require.Empty(t, vs.writes, "chunkstore hit must not trigger inline-to-OOB promotion")
}

func TestResolveFieldToLegacy_ZeroLeading20Bytes_ChunkstoreMiss(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()

	var raw [hash.ByteLen]byte
	raw[0] = 0x00
	for i := 1; i < hash.ByteLen; i++ {
		raw[i] = byte(i + 50)
	}
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	out, isLegacy, changed, err := resolveFieldToLegacy(ctx, raw[:], vs, cs)
	require.NoError(t, err)
	require.False(t, isLegacy, "chunkstore miss => adaptive inline => not legacy")
	require.True(t, changed, "chunkstore miss must trigger inline-to-OOB promotion")
	require.Len(t, vs.writes, 1, "chunkstore must receive the 19-byte inline content")
	require.Equal(t, raw[1:], vs.writes[0], "chunkstore content must equal the inline payload (v[1:])")
	// The new field is the hash of the inline content (the 19 bytes), not the
	// hash of the original 20-byte field.
	expectedHash := hash.Of(raw[1:])
	require.Equal(t, expectedHash[:], out)
}

// TestResolveFieldToLegacy_UnknownShape covers the abort cases: bytes that
// don't match any of the four valid shapes must produce an error so the outer
// migration can stop and report the offending row's primary key.
func TestResolveFieldToLegacy_UnknownShape(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	// Non-zero leader, 19 bytes — too short for legacy (20) and adaptive
	// addressed (21+).
	tooShort := bytes.Repeat([]byte{0x01}, 19)
	out, isLegacy, changed, err := resolveFieldToLegacy(ctx, tooShort, vs, cs)
	require.Error(t, err)
	require.False(t, isLegacy)
	require.False(t, changed)
	require.Nil(t, out)
	require.Contains(t, err.Error(), "unknown shape")

	// Non-zero leader, 21+ bytes, but malformed adaptive trailer. Construct:
	// leader 0x05 = 1-byte varint (declared length 5), then 10 bytes of
	// zero. Parser expects total = 1 + 20 = 21 bytes; we give 11. Should
	// abort.
	malformed := append([]byte{0x05}, bytes.Repeat([]byte{0x00}, 10)...)
	out, isLegacy, changed, err = resolveFieldToLegacy(ctx, malformed, vs, cs)
	require.Error(t, err)
	require.False(t, isLegacy)
	require.False(t, changed)
	require.Nil(t, out)
}

// TestResolveFieldToLegacy_ChunkstoreWriteError verifies that a chunkstore
// failure during inline-to-OOB promotion propagates as an error rather than
// silently dropping the content.
func TestResolveFieldToLegacy_ChunkstoreWriteError(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	vs.writeErr = errors.New("simulated chunkstore failure")
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	field := append([]byte{0x00}, []byte("payload")...)

	_, _, _, err := resolveFieldToLegacy(ctx, field, vs, cs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated chunkstore failure")
}

// TestResolveFieldToLegacy_ChunkstoreHasError verifies that a chunkstore
// presence-check failure propagates rather than silently treating it as a
// miss (which would corrupt a legacy-raw-hash row with a 0x00 leader).
func TestResolveFieldToLegacy_ChunkstoreHasError(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	cs := &stubChunkPresence{hasErr: errors.New("simulated presence-check failure")}

	var raw [hash.ByteLen]byte
	raw[0] = 0x00
	for i := 1; i < hash.ByteLen; i++ {
		raw[i] = byte(i)
	}

	_, _, _, err := resolveFieldToLegacy(ctx, raw[:], vs, cs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "presence check")
}

// TestResolveFieldToLegacy_RoundTripVerifiesContent is the integration-style
// unit test: write some inline content via resolveFieldToLegacy, then read
// it back via the resolver's reverse operation (vs.ReadBytes on the returned
// hash). The byte sequences must match.
func TestResolveFieldToLegacy_RoundTripVerifiesContent(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	// Three different inline payloads, including the degenerate empty case.
	cases := [][]byte{
		nil, // not actually tested here (NULL path is separate)
		{},
		[]byte("short"),
		bytes.Repeat([]byte{'x'}, 1024),
	}
	for i, payload := range cases {
		if i == 0 {
			continue // NULL handled by a different branch
		}
		inline := append([]byte{0x00}, payload...)
		out, _, changed, err := resolveFieldToLegacy(ctx, inline, vs, cs)
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, hash.ByteLen, len(out))
		h := hash.New(out)
		got, readErr := vs.ReadBytes(ctx, h)
		require.NoError(t, readErr)
		// Compare byte-wise (treat nil and empty as equivalent — both
		// represent "no content").
		require.True(t, bytes.Equal(payload, got),
			"round-tripped content must match the original inline payload (i=%d): want=%q got=%q", i, payload, got)
	}
}
