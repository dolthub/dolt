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
	"testing"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/stretchr/testify/require"
)

// TestJsonCursorPredecessorReads checks boundary keys without loading preceding Blob leaves.
func TestJsonCursorPredecessorReads(t *testing.T) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	leaves := make([]*Node, 4)
	for i, fragment := range []string{`{"a":1`, `,"b":2`, `,"c":3`, `,"d":4}`} {
		leaves[i], _ = writeBlobLeaf(t, ctx, ns, []byte(fragment))
	}
	keys := [][]byte{
		jsonLocationKey{byte(endOfValue)}.appendObjectKey([]byte("a")),
		jsonLocationKey{byte(endOfValue)}.appendObjectKey([]byte("b")),
		jsonLocationKey{byte(endOfValue)}.appendObjectKey([]byte("c")),
		{byte(endOfValue)},
	}
	writeMap := func(level int, keys [][]byte, children ...*Node) *Node {
		refs := make([][]byte, len(children))
		counts := make([]uint64, len(children))
		for i, child := range children {
			addr := child.HashOf()
			refs[i] = addr[:]
			count, err := child.TreeCount()
			require.NoError(t, err)
			counts[i] = uint64(count)
		}
		data := message.NewAddressMapSerializer(ns.Pool()).Serialize(keys, refs, counts, level)
		node, _, err := NodeFromBytes(data)
		require.NoError(t, err)
		_, err = ns.Write(ctx, node)
		require.NoError(t, err)
		return node
	}
	maps := []*Node{writeMap(1, keys[:2], leaves[:2]...), writeMap(1, keys[2:], leaves[2:]...)}
	root := writeMap(2, [][]byte{keys[1], keys[3]}, maps...)
	for _, test := range []struct {
		name        string
		leaf, reads int
		want        []byte
	}{
		{"first leaf", 0, 0, nil},
		{"same address map", 1, 0, keys[0]},
		{"previous address map", 2, 1, keys[1]},
		{"last leaf", 3, 0, keys[2]},
	} {
		t.Run(test.name, func(t *testing.T) {
			counting := &countingNodeStore{NodeStore: ns}
			top := &cursor{nd: root, idx: test.leaf / 2, nrw: counting}
			parent := &cursor{nd: maps[test.leaf/2], idx: test.leaf % 2, parent: top, nrw: counting}
			leaf := &cursor{nd: leaves[test.leaf], parent: parent, nrw: counting}
			before := leaf.clone()
			key, err := getPreviousKey(ctx, leaf)
			require.NoError(t, err)
			require.Equal(t, test.want, key)
			require.Equal(t, test.reads, counting.reads)
			for actual, original := leaf, before; actual != nil; actual, original = actual.parent, original.parent {
				require.Equal(t, original.idx, actual.idx)
				require.Same(t, original.nd, actual.nd)
			}
		})
	}
	t.Run("single leaf", func(t *testing.T) {
		counting := &countingNodeStore{NodeStore: ns}
		key, err := getPreviousKey(ctx, &cursor{nd: leaves[0], nrw: counting})
		require.NoError(t, err)
		require.Nil(t, key)
		require.Zero(t, counting.reads)
	})
	t.Run("future boundary", func(t *testing.T) {
		future := writeMap(1, [][]byte{{byte(jsonPathTypeNumElements + 1)}, keys[3]}, leaves[0], leaves[1])
		counting := &countingNodeStore{NodeStore: ns}
		leaf := &cursor{nd: leaves[1], nrw: counting, parent: &cursor{nd: future, idx: 1, nrw: counting}}
		_, err := getPreviousKey(ctx, leaf)
		require.ErrorIs(t, err, unknownLocationKeyError)
		require.Zero(t, counting.reads)
	})
	for _, test := range []struct {
		name   string
		node   *Node
		parent *cursor
		want   []byte
		reads  int
	}{
		{"root offset", root, nil, keys[1], 3},
		{"internal offset", maps[1], &cursor{nd: root, idx: 1}, keys[2], 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			counting := &countingNodeStore{NodeStore: ns}
			if test.parent != nil {
				test.parent.nrw = counting
			}
			input := &cursor{nd: test.node, idx: 1, parent: test.parent, nrw: counting}
			got, err := newJsonCursorFromCursor(ctx, input)
			require.NoError(t, err)
			require.Equal(t, test.want, []byte(got.jsonScanner.currentPath.key))
			require.Equal(t, test.reads, counting.reads)
			require.Equal(t, 1, input.idx)
		})
	}
}
