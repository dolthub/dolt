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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
)

// TestKeyAddressBookkeepingInChunkedTrees builds a real multi-level tree through the chunker with
// every key stored out of band, and verifies that:
//   - every node, leaf and internal alike, records the addresses embedded in its key tuples in its
//     key_address_offsets field, making each node self-contained in its description of which of its
//     keys reference out-of-band values
//   - the address walk used by gc, push, and clone reaches every out-of-band key chunk exactly once,
//     through the leaves only: internal node entries are bookkeeping and are not walked (each boundary
//     key is a copy of a leaf key below it)
func TestKeyAddressBookkeepingInChunkedTrees(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()

	kd := val.NewTupleDescriptor(val.Type{Enc: val.StringAdaptiveEnc, Nullable: true})
	vd := val.NewTupleDescriptor(val.Type{Enc: val.StringAdaptiveEnc, Nullable: true})

	// build a tree with 500 out-of-band keys: enough key tuples (each holding a ~22 byte address
	// encoding) to split into several leaves under an internal root
	const count = 500
	expectedAddrs := hash.NewHashSet()
	serializer := message.NewProllyMapSerializer(kd, vd, ns.Pool())
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)
	for i := 0; i < count; i++ {
		content := fmt.Sprintf("%04d", i) + strings.Repeat("k", 3000)
		oob, err := val.NewOutOfBandAdaptiveValue(ctx, ns, []byte(content))
		require.NoError(t, err)
		addr, err := oob.OutOfBandAddr()
		require.NoError(t, err)
		expectedAddrs.Insert(addr)

		key := val.NewTuple(ns.Pool(), oob)
		value := val.NewTuple(ns.Pool(), append([]byte{0}, fmt.Sprintf("val-%04d", i)...))
		require.NoError(t, chkr.AddPair(ctx, Item(key), Item(value)))
	}
	root, err := chkr.Done(ctx)
	require.NoError(t, err)
	require.Equal(t, count, expectedAddrs.Size(), "expected unique out-of-band content per key")
	require.GreaterOrEqual(t, root.Level(), 1, "test requires a multi-level tree")

	// every node records the addresses embedded in its keys, at every level
	leafRecorded := hash.NewHashSet()
	var internalNodes, leafNodes int
	err = WalkNodes(ctx, root, ns, func(ctx context.Context, nd *Node) error {
		var pm serial.ProllyTreeNode
		require.NoError(t, serial.InitProllyTreeNodeRoot(&pm, nd.bytes(), serial.MessagePrefixSz))
		// every key in this tree is out of band, so every key contributes one recorded address
		require.Equal(t, nd.Count(), pm.KeyAddressOffsetsLength(),
			"node at level %d should record one address per key", nd.Level())

		keyItems := pm.KeyItemsBytes()
		for i := 0; i < pm.KeyAddressOffsetsLength(); i++ {
			o := pm.KeyAddressOffsets(i)
			addr := hash.New(keyItems[o : o+hash.ByteLen])
			assert.True(t, expectedAddrs.Has(addr), "recorded address %s is not a key chunk", addr)
			if nd.IsLeaf() {
				leafRecorded.Insert(addr)
			}
		}
		if nd.IsLeaf() {
			leafNodes++
		} else {
			internalNodes++
		}
		return nil
	})
	require.NoError(t, err)
	require.NotZero(t, internalNodes)
	require.Greater(t, leafNodes, 1)
	require.Equal(t, count, leafRecorded.Size(), "every key chunk should be recorded in some leaf")

	// the address walk reaches every out-of-band key chunk exactly once, via the leaves: internal
	// node bookkeeping entries are not walked
	visits := make(map[hash.Hash]int)
	err = WalkAddresses(ctx, root, ns, func(_ context.Context, addr hash.Hash) error {
		visits[addr]++
		return nil
	})
	require.NoError(t, err)
	for addr := range expectedAddrs {
		assert.Equal(t, 1, visits[addr], "key chunk %s should be visited exactly once", addr)
	}
}
