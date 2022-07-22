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
	0xde, 0x2c, 0x34, 0x6a, 0xb3,
	0x88, 0x6f, 0x95, 0xb, 0x3b,
	0x77, 0xa1, 0x1f, 0xfa, 0x4c,
	0xf1, 0xe8, 0xdb, 0xfa, 0xcc,
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
