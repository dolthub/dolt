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

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func MustProllyMapFromTuples(t *testing.T, kd, vd val.TupleDesc, tuples [][2]val.Tuple) Map {
	ctx := context.Background()
	ns := tree.NewTestNodeStore()

	serializer := message.ProllyMapSerializer{Pool: ns.Pool()}
	chunker, err := tree.NewEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for _, pair := range tuples {
		err := chunker.AddPair(ctx, tree.Item(pair[0]), tree.Item(pair[1]))
		require.NoError(t, err)
	}
	root, err := chunker.Done(ctx)
	require.NoError(t, err)

	return NewMap(root, ns, kd, vd)
}
