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
	0xcb, 0x1d, 0xe1, 0xbd, 0x4a,
	0x3c, 0x27, 0x18, 0xe, 0xba,
	0xb7, 0xe8, 0x30, 0x44, 0x69,
	0x3d, 0x86, 0x27, 0x26, 0x2d,
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
