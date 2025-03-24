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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func TestArtifactMapEditing(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()

	var srcKd = val.NewTupleDescriptor(val.Type{Enc: val.Int16Enc})
	var srcKb = val.NewTupleBuilder(srcKd, ns)

	am, err := NewArtifactMapFromTuples(ctx, ns, srcKd)
	require.NoError(t, err)

	addr, err := ns.Write(ctx, tree.NewEmptyTestNode())
	require.NoError(t, err)

	for _, n := range []int{10, 100, 1000} {
		t.Run(fmt.Sprintf("%d inserts", n), func(t *testing.T) {
			edt := am.Editor()
			for i := 0; i < n; i++ {
				srcKb.PutInt16(0, int16(i))
				key1 := srcKb.Build(sharedPool)
				err = edt.Add(ctx, key1, addr, ArtifactTypeConflict, []byte("{}"))
				require.NoError(t, err)
			}
			nm, err := edt.Flush(ctx)
			require.NoError(t, err)

			nodeCount := 0
			err = nm.WalkNodes(ctx, func(_ context.Context, nd tree.Node) error {
				nodeCount++
				return nil
			})
			require.NoError(t, err)

			addressCount := 0
			err = nm.WalkAddresses(ctx, func(ctx context.Context, addr hash.Hash) error {
				addressCount++
				return nil
			})

			// Verify that we found all the Root-ish hashes
			if nodeCount == 1 {
				assert.Equal(t, n, addressCount)
			} else {
				assert.Equal(t, n, addressCount-nodeCount+1)
			}
		})
	}
}

// Smoke test for merging artifact maps
func TestMergeArtifactMaps(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()

	var srcKd = val.NewTupleDescriptor(val.Type{Enc: val.Int16Enc})
	var srcKb = val.NewTupleBuilder(srcKd, ns)

	base, err := NewArtifactMapFromTuples(ctx, ns, srcKd)
	require.NoError(t, err)
	left, err := NewArtifactMapFromTuples(ctx, ns, srcKd)
	require.NoError(t, err)
	right, err := NewArtifactMapFromTuples(ctx, ns, srcKd)
	require.NoError(t, err)
	expected, err := NewArtifactMapFromTuples(ctx, ns, srcKd)
	require.NoError(t, err)

	addr, err := ns.Write(ctx, tree.NewEmptyTestNode())
	require.NoError(t, err)

	leftEdt := left.Editor()
	rightEdt := right.Editor()

	srcKb.PutInt16(0, 1)
	key1 := srcKb.Build(sharedPool)
	err = leftEdt.Add(ctx, key1, addr, ArtifactTypeConflict, []byte("{}"))
	require.NoError(t, err)
	left, err = leftEdt.Flush(ctx)
	require.NoError(t, err)

	srcKb.PutInt16(0, 2)
	key2 := srcKb.Build(sharedPool)
	err = rightEdt.Add(ctx, key2, addr, ArtifactTypeConflict, []byte("{}"))
	require.NoError(t, err)
	right, err = rightEdt.Flush(ctx)

	expectedEdt := expected.Editor()
	err = expectedEdt.Add(ctx, key1, addr, ArtifactTypeConflict, []byte("{}"))
	require.NoError(t, err)
	err = expectedEdt.Add(ctx, key2, addr, ArtifactTypeConflict, []byte("{}"))
	require.NoError(t, err)
	expected, err = expectedEdt.Flush(ctx)

	merged, err := MergeArtifactMaps(ctx, left, right, base, func(left, right tree.Diff) (tree.Diff, bool) {
		t.Fatalf("collision not expected")
		return tree.Diff{}, false
	})
	require.NoError(t, err)

	assert.Equal(t, expected.HashOf(), merged.HashOf())
	es, err := ArtifactDebugFormat(ctx, expected)
	require.NoError(t, err)
	ms, err := ArtifactDebugFormat(ctx, merged)
	require.NoError(t, err)

	t.Log(es)
	t.Log(ms)

	assert.Equal(t, es, ms)
}
