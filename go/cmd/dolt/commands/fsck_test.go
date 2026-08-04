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

package commands

import (
	"context"
	"testing"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

// TestResolveStashRefsCorruptStashList exercises resolveStashRefs against damaged stash lists. fsck exists to inspect
// potentially corrupted repositories, so reading a stash list must report integrity errors rather than panic.
func TestResolveStashRefsCorruptStashList(t *testing.T) {
	ctx := context.Background()
	headHash := mkTestHash(1)
	rootHash := mkTestHash(2)

	t.Run("non-numeric address map key does not panic", func(t *testing.T) {
		// A stash's address map key is normally its numeric index. A corrupt, non-numeric key used to crash the
		// datas stash helpers (strconv.Atoi failure was swallowed, leaving a nil entry that was then dereferenced).
		// resolveStashRefs walks the address map directly and does not depend on the key, so it must still resolve.
		cs, ns := newTestChunkStore(t)
		stashAddr := writeTestChunk(t, ctx, cs, buildTestStash(headHash, rootHash))
		listAddr := writeTestChunk(t, ctx, cs, buildTestStashList(t, ctx, ns, "not-a-number", stashAddr))

		var errs Errs
		heads, roots := resolveStashRefs(ctx, cs, ns, listAddr, "refs/stashes/dolt", &errs)

		require.Empty(t, errs)
		require.Equal(t, []hash.Hash{headHash}, heads)
		require.Equal(t, []hash.Hash{rootHash}, roots)
	})

	t.Run("stash entry pointing to a missing object reports an error", func(t *testing.T) {
		cs, ns := newTestChunkStore(t)
		missingStash := mkTestHash(9)
		listAddr := writeTestChunk(t, ctx, cs, buildTestStashList(t, ctx, ns, "0", missingStash))

		var errs Errs
		heads, roots := resolveStashRefs(ctx, cs, ns, listAddr, "refs/stashes/dolt", &errs)

		require.Len(t, errs, 1)
		require.Empty(t, heads)
		require.Empty(t, roots)
	})
}

func newTestChunkStore(t *testing.T) (chunks.ChunkStore, tree.NodeStore) {
	stg := &chunks.MemoryStorage{}
	cs := stg.NewViewWithDefaultFormat()
	return cs, tree.NewNodeStore(cs)
}

func writeTestChunk(t *testing.T, ctx context.Context, cs chunks.ChunkStore, data []byte) hash.Hash {
	c := chunks.NewChunk(data)
	noRefs := func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	}
	require.NoError(t, cs.Put(ctx, c, noRefs))
	return c.Hash()
}

func mkTestHash(b byte) hash.Hash {
	var h hash.Hash
	h[hash.ByteLen-1] = b
	return h
}

func buildTestStash(head, root hash.Hash) []byte {
	b := flatbuffers.NewBuilder(1024)
	rootOff := b.CreateByteVector(root[:])
	headOff := b.CreateByteVector(head[:])
	branchOff := b.CreateString("main")
	descOff := b.CreateString("WIP")
	serial.StashStart(b)
	serial.StashAddStashRootAddr(b, rootOff)
	serial.StashAddHeadCommitAddr(b, headOff)
	serial.StashAddBranchName(b, branchOff)
	serial.StashAddDesc(b, descOff)
	return []byte(serial.FinishMessage(b, serial.StashEnd(b), []byte(serial.StashFileID)))
}

func buildTestStashList(t *testing.T, ctx context.Context, ns tree.NodeStore, key string, stashAddr hash.Hash) []byte {
	am, err := prolly.NewEmptyAddressMap(ns)
	require.NoError(t, err)
	editor := am.Editor()
	require.NoError(t, editor.Add(ctx, key, stashAddr))
	am, err = editor.Flush(ctx)
	require.NoError(t, err)

	amBytes := []byte(tree.ValueFromNode(am.Node()).(types.SerialMessage))
	b := flatbuffers.NewBuilder(1024)
	amOff := b.CreateByteVector(amBytes)
	serial.StashListStart(b)
	serial.StashListAddAddressMap(b, amOff)
	return []byte(serial.FinishMessage(b, serial.StashListEnd(b), []byte(serial.StashListFileID)))
}
