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

var expected = hash.Hash{
	0x2f, 0x3f, 0xbe, 0xff, 0x61,
	0x91, 0xd2, 0x95, 0x82, 0xdb,
	0xc9, 0xca, 0x5d, 0xcd, 0xf7,
	0x63, 0x41, 0xd5, 0xaa, 0x4c,
}

func TestContentAddress(t *testing.T) {
	keys, values := ascendingIntTuples(t, 12345)
	m := makeTree(t, keys, values)
	require.NotNil(t, m)
	require.Equal(t, expected, m.hashOf())
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
