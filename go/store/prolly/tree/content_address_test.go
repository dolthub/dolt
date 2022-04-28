// Copyright 2022 Dolthub, Inc.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

var goldenHash = hash.Hash{
	0x0, 0x96, 0xe6, 0x34, 0x5e,
	0xe3, 0x57, 0x41, 0xce, 0x8e,
	0x4, 0xc2, 0x35, 0x7c, 0xe3,
	0xa6, 0xaa, 0xe5, 0x2b, 0x60,
}

func TestContentAddress(t *testing.T) {
	tups, _ := AscendingUintTuples(12345)
	m := makeTree(t, tups)
	require.NotNil(t, m)
	require.Equal(t, goldenHash, m.HashOf())
	assert.Equal(t, 12345, m.TreeCount())
}

func makeTree(t *testing.T, tuples [][2]val.Tuple) Node {
	ctx := context.Background()
	ns := NewTestNodeStore()

	chunker, err := newEmptyTreeChunker(ctx, ns, defaultSplitterFactory)
	require.NoError(t, err)
	for _, pair := range tuples {
		err := chunker.AddPair(ctx, NodeItem(pair[0]), NodeItem(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)
	return root
}
