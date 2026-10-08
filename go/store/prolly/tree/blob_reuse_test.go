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

package tree

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

type writeCountingNodeStore struct {
	NodeStore
	writes int
}

func (c *writeCountingNodeStore) Write(ctx context.Context, nd *Node) (hash.Hash, error) {
	c.writes++
	return c.NodeStore.Write(ctx, nd)
}

func randomBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func TestSerializeBytesToAddrReusingMatchesFreshBuild(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	old := randomBytes(1<<20, 1)
	_, oldAddr, err := SerializeBytesToAddr(ctx, ns, bytes.NewReader(old), len(old))
	require.NoError(t, err)

	oneLeaf := bytes.Clone(old)
	oneLeaf[500000] ^= 0xff
	longer := append(bytes.Clone(old), randomBytes(10000, 2)...)

	for name, data := range map[string][]byte{
		"unchanged":        old,
		"one leaf changed": oneLeaf,
		"appended":         longer,
		"truncated":        old[:600000],
		"replaced":         randomBytes(1<<20, 3),
		"single leaf":      randomBytes(100, 4),
	} {
		t.Run(name, func(t *testing.T) {
			_, want, err := SerializeBytesToAddr(ctx, ns, bytes.NewReader(data), len(data))
			require.NoError(t, err)
			got, err := SerializeBytesToAddrReusing(ctx, ns, data, oldAddr)
			require.NoError(t, err)
			require.Equal(t, want, got)

			read, err := ns.ReadBytes(ctx, got)
			require.NoError(t, err)
			require.Equal(t, data, read)
		})
	}
}

func TestSerializeBytesToAddrReusingWritesOnlyChangedLeaves(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	old := randomBytes(1<<20, 1)
	_, oldAddr, err := SerializeBytesToAddr(ctx, ns, bytes.NewReader(old), len(old))
	require.NoError(t, err)
	leaves, err := blobLeaves(ctx, ns, oldAddr)
	require.NoError(t, err)

	data := bytes.Clone(old)
	data[500000] ^= 0xff

	build := func(prior []priorLeaf) int {
		counting := &writeCountingNodeStore{NodeStore: ns}
		bb := ns.BlobBuilder()
		defer ns.PutBlobBuilder(bb)
		bb.SetNodeStore(counting)
		bb.Init(len(data))
		bb.prior = prior
		_, _, err := bb.Chunk(ctx, bytes.NewReader(data))
		require.NoError(t, err)
		return counting.writes
	}
	fresh := build(nil)
	reusing := build(leaves)
	require.Equal(t, fresh-(len(leaves)-1), reusing, "only the changed leaf and interior nodes are written")
}
