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
	0x14, 0xc8, 0x30, 0xe0, 0xc7,
	0x84, 0xe8, 0xc8, 0x78, 0x45,
	0xa5, 0x4f, 0x94, 0xe7, 0x52,
	0x7b, 0x17, 0x21, 0x3f, 0x59,
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
