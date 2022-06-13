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
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
)

var goldenHash = hash.Hash{
	0x39, 0x1c, 0xcb, 0xd8,
	0xea, 0xd2, 0xdc, 0x42,
	0x76, 0xb7, 0x38, 0xf1,
	0x0d, 0x4f, 0x48, 0x91,
	0x7d, 0x1f, 0xb7, 0xb4,
}

// todo(andy): need and analogous test in pkg prolly
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

	// todo(andy): move this test
	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	chunker, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)
	for _, pair := range tuples {
		err := chunker.AddPair(ctx, Item(pair[0]), Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)
	return root
}
