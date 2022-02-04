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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

var expected = hash.Hash{
	0xa2, 0x81, 0x3b, 0xf, 0xdb,
	0x2f, 0x27, 0x1d, 0x60, 0x86,
	0x22, 0x97, 0x0, 0x86, 0x9b,
	0x6e, 0x55, 0xb2, 0xec, 0x5c,
}

func TestContentAddress(t *testing.T) {
	keys, values := ascendingIntPairs(t, 12345)
	m := makeTree(t, keys, values)
	require.Equal(t, expected, m.hashOf())
}

func makeTree(t *testing.T, keys, values []nodeItem) Node {
	ctx := context.Background()
	ns := newTestNodeStore()

	chunker, err := newEmptyTreeChunker(ctx, ns, newDefaultNodeSplitter)
	require.NoError(t, err)
	for i := range keys {
		_, err := chunker.Append(ctx, keys[i], values[i])
		require.NoError(t, err)
	}

	root, err := chunker.Done(ctx)
	require.NoError(t, err)
	return root
}
