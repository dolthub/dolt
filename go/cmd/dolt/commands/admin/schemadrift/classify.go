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
	"context"

	"github.com/mohae/uvarint"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// ChunkPresenceChecker abstracts the "is this hash a real chunk?" lookup used
// to disambiguate the 0x00-leading-byte case (where a legacy raw-hash field
// and an adaptive-inline-with-empty-payload field have the same first byte).
// dEnv.DoltDB(ctx) satisfies this interface at runtime; tests substitute
// fakes.
type ChunkPresenceChecker interface {
	Has(ctx context.Context, h hash.Hash) (bool, error)
}

// FieldFormat classifies the on-disk byte layout of a single tuple field,
// without invoking any adaptive-encoding dispatch. We classify byte-by-byte so
// the classifier itself cannot panic on a corrupted field — the whole point of
// this command is to *find* the corruption that would panic the normal reader.
//
// The four mutually-exclusive categories are:
//
//   - FieldNULL              — empty slice (val.Tuple.GetField returned nil)
//   - FieldAdaptiveInline    — leading byte 0x00 (genuine adaptive inline tag)
//   - FieldAdaptiveAddressed — non-zero leading byte AND total length =
//     varint(declared_length) + hash.ByteLen, where the varint is the SQLite4
//     length prefix described by `val.AdaptiveValue` (always >= 21 bytes)
//   - FieldLegacyRawHash     — exactly hash.ByteLen (20) bytes, non-zero
//     leading byte. This is what `StringAddrEnc`/`BytesAddrEnc`/`JSONAddrEnc`/
//     `GeomAddrEnc` wrote in 1.x — a bare 20-byte address with no length
//     prefix. A 20-byte adaptive value would necessarily start with 0x00
//     (1-byte inline header + 19 inline bytes), so a 20-byte non-zero-leading
//     field is unambiguously legacy.
//   - FieldUnknown           — anything else: 1-19 bytes, or 21+ bytes whose
//     declared length doesn't match the trailer. We never auto-classify these
//     as drift; they're surfaced for human triage.
type FieldFormat int

const (
	FieldUnknown FieldFormat = iota
	FieldNULL
	FieldAdaptiveInline
	FieldAdaptiveAddressed
	FieldLegacyRawHash
)

// String returns a stable, lowercased name suitable for table / JSON output.
func (f FieldFormat) String() string {
	switch f {
	case FieldNULL:
		return "null"
	case FieldAdaptiveInline:
		return "adaptive-inline"
	case FieldAdaptiveAddressed:
		return "adaptive-addressed"
	case FieldLegacyRawHash:
		return "legacy-raw-hash"
	default:
		return "unknown"
	}
}

// ClassifyFieldBytes inspects a single field's persisted bytes (as returned by
// val.Tuple.GetField for a non-virtual column) and returns its FieldFormat.
//
// Crucially, this routine NEVER calls into adaptive dispatch — that is the
// dispatch that panics with `invalid hash length: 19` on legacy raw-hash data
// loaded under an adaptive-tagged schema. It also doesn't depend on the
// declared schema at all; the schema is consulted by the caller to decide
// whether the result represents drift (schema says adaptive, payload says
// legacy) vs. an internally consistent column.
func ClassifyFieldBytes(b []byte) FieldFormat {
	if len(b) == 0 {
		return FieldNULL
	}
	if b[0] == 0 {
		// Adaptive inline format: [0x00][inline bytes...]. The inline payload
		// can be any length, including the degenerate 0-byte case.
		return FieldAdaptiveInline
	}
	// Leading byte is non-zero. Two possibilities:
	//   1. Genuine adaptive-addressed value: varint(declared_length, >=1 byte) +
	//      hash.ByteLen address bytes. Total >= 21.
	//   2. Legacy raw 20-byte hash address (StringAddrEnc et al.). Total = 20,
	//      leading byte != 0.
	if len(b) == hash.ByteLen {
		// 20 bytes, non-zero leader. Cannot be adaptive — adaptive needs at
		// minimum 1 varint byte + 20 hash bytes = 21. So this is unambiguously
		// legacy.
		return FieldLegacyRawHash
	}
	if len(b) > hash.ByteLen {
		// Try to read it as an adaptive-addressed value. Parse the varint
		// prefix and check the trailer length matches.
		declaredLen, varintSize := uvarint.Uvarint(b)
		if varintSize > 0 && varintSize+hash.ByteLen == len(b) {
			// The trailer is exactly hash.ByteLen after a clean varint —
			// matches the adaptive-addressed shape spelled out in
			// val.AdaptiveValue's doc comment. The declared length is a
			// reasonableness signal only (we don't deref the address to
			// confirm it because that would need a chunkstore round-trip and
			// would defeat the no-side-effects contract of check).
			_ = declaredLen
			return FieldAdaptiveAddressed
		}
	}
	return FieldUnknown
}

// IsLegacyEncoding reports whether the given val.Encoding corresponds to the
// pre-adaptive (1.x) family for any TEXT / BLOB / JSON / GEOMETRY column. The
// repair path uses this to derive the target encoding from the drifted
// adaptive sibling, and the check path uses it to suppress noise from columns
// already at the legacy tag (which by definition cannot be drifted, because
// drift means "schema says adaptive, payload says legacy").
func IsLegacyEncoding(enc val.Encoding) bool {
	switch enc {
	case val.StringAddrEnc,
		val.BytesAddrEnc,
		val.JSONAddrEnc,
		val.GeomAddrEnc,
		val.ExtendedAddrEnc:
		return true
	}
	return false
}

// IsAdaptiveEncoding reports whether the given val.Encoding corresponds to
// the v2.0.7+ adaptive family for any TEXT / BLOB / JSON / GEOMETRY column.
// These are the encodings whose schema records can drift under the regression
// the rest of this branch fixes — and whose on-disk row layouts the repair
// command may need to inspect for legacy-raw-hash bytes.
func IsAdaptiveEncoding(enc val.Encoding) bool {
	switch enc {
	case val.StringAdaptiveEnc,
		val.BytesAdaptiveEnc,
		val.JsonAdaptiveEnc,
		val.GeomAdaptiveEnc,
		val.ExtendedAdaptiveEnc:
		return true
	}
	return false
}

// ClassifyFieldWithChunkstore is the disambiguating counterpart to
// ClassifyFieldBytes for the load-bearing 0x00-leading-byte case. A field of
// length exactly hash.ByteLen whose first byte is 0x00 is structurally
// ambiguous: it could be an adaptive-inline value with one prefix byte and 19
// inline bytes, OR a legacy raw-hash field that happens to begin with 0x00
// (~1 in 256 hashes). The structural classifier alone can't tell them apart
// and consequently misses ~1/256 of legacy rows — exactly the kind of silent
// false-negative the operator pushback flagged as unacceptable.
//
// This routine adds a chunkstore presence check: if the field interpreted as
// hash.New(field) resolves to a stored chunk, the field is a legacy raw hash.
// Otherwise (and otherwise only) it's adaptive inline. The lookup is a single
// presence check, not a chunk fetch — it's fast and never triggers any
// adaptive dispatch.
//
// For NON-ambiguous shapes (length != 20, leading byte != 0, etc.) this
// routine delegates to ClassifyFieldBytes and never touches the chunkstore.
func ClassifyFieldWithChunkstore(ctx context.Context, cs ChunkPresenceChecker, b []byte) (FieldFormat, error) {
	f := ClassifyFieldBytes(b)
	if f != FieldAdaptiveInline {
		return f, nil
	}
	// Adaptive-inline classification — is it possibly a legacy raw hash with
	// a 0x00 leading byte instead? Only the 20-byte case is ambiguous; a 1- or
	// 100-byte payload starting with 0x00 cannot be a legacy raw hash.
	if len(b) != hash.ByteLen {
		return f, nil
	}
	if cs == nil {
		// No chunkstore available (e.g. structural-only test path). Trust
		// the structural classification.
		return f, nil
	}
	h := hash.New(b)
	present, err := cs.Has(ctx, h)
	if err != nil {
		return f, err
	}
	if present {
		return FieldLegacyRawHash, nil
	}
	return f, nil
}

// LegacySibling returns the pre-adaptive sibling encoding for an adaptive
// encoding (the encoding `repair` will flip the persisted schema record to).
// For a non-adaptive input the call returns (0, false). The pairing is the
// single source of truth used by both `check`'s display column and `repair`'s
// schema mutation.
func LegacySibling(adaptive val.Encoding) (val.Encoding, bool) {
	switch adaptive {
	case val.StringAdaptiveEnc:
		return val.StringAddrEnc, true
	case val.BytesAdaptiveEnc:
		return val.BytesAddrEnc, true
	case val.JsonAdaptiveEnc:
		return val.JSONAddrEnc, true
	case val.GeomAdaptiveEnc:
		return val.GeomAddrEnc, true
	case val.ExtendedAdaptiveEnc:
		return val.ExtendedAddrEnc, true
	}
	return 0, false
}

// AdaptiveSibling returns the v2-native adaptive sibling encoding for a legacy
// (pre-adaptive) TEXT / BLOB / JSON / GEOMETRY encoding — the encoding the
// forward `migrate-adaptive` heal flips the persisted schema record to. For a
// non-legacy input the call returns (0, false). This is the exact inverse of
// LegacySibling and the single source of truth used by the forward migration.
func AdaptiveSibling(legacy val.Encoding) (val.Encoding, bool) {
	switch legacy {
	case val.StringAddrEnc:
		return val.StringAdaptiveEnc, true
	case val.BytesAddrEnc:
		return val.BytesAdaptiveEnc, true
	case val.JSONAddrEnc:
		return val.JsonAdaptiveEnc, true
	case val.GeomAddrEnc:
		return val.GeomAdaptiveEnc, true
	case val.ExtendedAddrEnc:
		return val.ExtendedAdaptiveEnc, true
	}
	return 0, false
}
