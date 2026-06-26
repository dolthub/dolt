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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

// TestResolveFieldToAdaptive_FailsLoudOnTransientReadError pins the read-error fix
// (the read-error fix): the migrate-path resolver must DISTINGUISH a genuine
// chunk-absence (recovered empty-chunk panic -> treat as inline) from a
// TRANSIENT read error (propagate / fail loud).
//
// A 20-byte 0x00-leading field is ambiguous: a legacy raw-hash address that
// coincidentally starts 0x00, OR an adaptive-inline value with 19 content
// bytes. resolveFieldToAdaptive performs an authoritative read to disambiguate.
// If that read returns a transient error, the resolver MUST return a non-nil
// error — NOT swallow it and silently inline the 20 hash bytes as "content"
// (which would corrupt a real legacy row by storing its address-bytes as data).
//
// This is the migrate-path analog of TestResolveFieldToLegacy_ChunkstoreHasError.
// FAILS-WITHOUT the read-error fix (an earlier switch swallowed read errors as inline);
// PASSES-WITH it.
func TestResolveFieldToAdaptive_FailsLoudOnTransientReadError(t *testing.T) {
	ctx := context.Background()
	vs := newStubVS()
	vs.readErr = errors.New("transient read failure")
	cs := &stubChunkPresence{present: map[hash.Hash]bool{}}

	// 20-byte field, 0x00 leader, non-zero remainder -> the ambiguous shape that
	// forces an authoritative disambiguating read.
	var raw [hash.ByteLen]byte
	raw[0] = 0x00
	for i := 1; i < hash.ByteLen; i++ {
		raw[i] = byte(i)
	}

	_, _, _, err := resolveFieldToAdaptive(ctx, raw[:], vs, cs)
	require.Error(t, err,
		"a transient read error during 0x00-lead disambiguation must propagate (fail loud), not be swallowed as inline")
	require.Contains(t, err.Error(), "transient read failure",
		"the propagated error must carry the underlying transient read failure")
}
