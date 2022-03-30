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

package prolly

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

var goldenHash = hash.Hash{
	0x0, 0x26, 0x55, 0xec, 0x3,
	0x30, 0x52, 0xed, 0xdc, 0x9a,
	0xdd, 0xe, 0x76, 0x4f, 0x3f,
	0x79, 0xe0, 0xdc, 0xfd, 0x41,
}

func TestContentAddress(t *testing.T) {
	keys, values := ascendingIntTuples(t, 12345)
	m := makeTree(t, keys, values)
	require.NotNil(t, m)
	require.Equal(t, goldenHash, m.hashOf())
	assert.Equal(t, 12345, m.treeCount())
}

func makeTree(t *testing.T, keys, values []val.Tuple) Node {
	ctx := context.Background()
	ns := newTestNodeStore()

	chunker, err := newEmptyTreeChunker(ctx, ns, newDefaultNodeSplitter)
	require.NoError(t, err)
	for i := range keys {
		err := chunker.AddPair(ctx, keys[i], values[i])
		require.NoError(t, err)
	}

	root, err := chunker.Done(ctx)
	require.NoError(t, err)
	return root
}
