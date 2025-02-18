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
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func TestCommitClosure(t *testing.T) {
	ns := tree.NewTestNodeStore()
	ctx := context.Background()

	t.Run("Keys", func(t *testing.T) {
		k0 := NewCommitClosureKey(ns.Pool(), 0, hash.Parse("00000000000000000000000000000000"))
		assert.Equal(t, uint64(0), k0.Height())
		assert.True(t, k0.Addr().Equal(hash.Hash{}))

		k0_ := NewCommitClosureKey(ns.Pool(), 0, hash.Parse("00000000000000000000000000000000"))
		assert.False(t, k0.Less(ctx, k0_))

		h := hash.Parse("00000000000000000000000000000001")
		k0_1 := NewCommitClosureKey(ns.Pool(), 0, h)
		assert.True(t, k0_1.Addr().Equal(h))
		assert.True(t, k0.Less(ctx, k0_1))
		assert.False(t, k0_1.Less(ctx, k0_))

	})

	t.Run("Empty", func(t *testing.T) {
		cc, err := NewEmptyCommitClosure(ns)
		require.NoError(t, err)
		assert.NotNil(t, cc)
		c, err := cc.Count()
		require.NoError(t, err)
		assert.Equal(t, 0, c)
		assert.Equal(t, 0, cc.closure.Root.Count())
		assert.Equal(t, 1, cc.Height())

		i, err := cc.IterAllReverse(ctx)
		_, _, err = i.Next(ctx)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, io.EOF))
	})

	t.Run("Insert", func(t *testing.T) {
		cc, err := NewEmptyCommitClosure(ns)
		require.NoError(t, err)
		addr, err := ns.Write(ctx, tree.NewEmptyTestNode())
		require.NoError(t, err)
		e := cc.Editor()
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 0, addr))
		assert.NoError(t, err)
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 1, addr))
		assert.NoError(t, err)
		cc, err = e.Flush(ctx)
		assert.NoError(t, err)
		ccc, err := cc.Count()
		require.NoError(t, err)
		assert.Equal(t, 2, ccc)

		i, err := cc.IterAllReverse(ctx)
		assert.NoError(t, err)
		k, _, err := i.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), k.Height())
		k, _, err = i.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), k.Height())
		_, _, err = i.Next(ctx)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, io.EOF))

		e = cc.Editor()
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 0, addr))
		assert.NoError(t, err)
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 1, addr))
		assert.NoError(t, err)
		cc, err = e.Flush(ctx)
		assert.NoError(t, err)
		ccc, err = cc.Count()
		require.NoError(t, err)
		assert.Equal(t, 2, ccc)
	})

	t.Run("Diff", func(t *testing.T) {
		ccl, err := NewEmptyCommitClosure(ns)
		require.NoError(t, err)
		addr, err := ns.Write(ctx, tree.NewEmptyTestNode())
		require.NoError(t, err)
		e := ccl.Editor()
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 0, addr))
		assert.NoError(t, err)
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 1, addr))
		assert.NoError(t, err)
		ccl, err = e.Flush(ctx)
		assert.NoError(t, err)
		cclc, err := ccl.Count()
		require.NoError(t, err)
		assert.Equal(t, 2, cclc)

		ccr, err := NewEmptyCommitClosure(ns)
		require.NoError(t, err)
		e = ccr.Editor()
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 0, addr))
		assert.NoError(t, err)
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 1, addr))
		assert.NoError(t, err)
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 1, addr))
		assert.NoError(t, err)
		err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), 2, addr))
		assert.NoError(t, err)
		ccr, err = e.Flush(ctx)
		assert.NoError(t, err)
		ccrc, err := ccr.Count()
		require.NoError(t, err)
		assert.Equal(t, 3, ccrc)

		var numadds, numdels int
		err = DiffCommitClosures(ctx, ccl, ccr, func(ctx context.Context, d tree.Diff) error {
			if d.Type == tree.AddedDiff {
				numadds++
			} else if d.Type == tree.RemovedDiff {
				numdels++
			}
			return nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, io.EOF))
		assert.Equal(t, 1, numadds)
		assert.Equal(t, 0, numdels)
	})

	t.Run("WalkAddresses", func(t *testing.T) {
		cc, err := NewEmptyCommitClosure(ns)
		require.NoError(t, err)
		e := cc.Editor()
		for i := 0; i < 4096; i++ {
			addr, err := ns.Write(ctx, tree.NewEmptyTestNode())
			require.NoError(t, err)
			err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), uint64(i), addr))
			require.NoError(t, err)
		}
		cc, err = e.Flush(ctx)
		require.NoError(t, err)
		ccc, err := cc.Count()
		require.NoError(t, err)
		assert.Equal(t, 4096, ccc)

		// Walk the addresses in the Root.
		msg := serial.Message(tree.ValueFromNode(cc.closure.Root).(types.SerialMessage))
		numaddresses := 0
		err = message.WalkAddresses(ctx, msg, func(ctx context.Context, addr hash.Hash) error {
			numaddresses++
			return nil
		})
		require.NoError(t, err)
		assert.Less(t, numaddresses, 4096)

		// Walk all addresses in the Tree.
		numaddresses = 0
		err = tree.WalkAddresses(ctx, cc.closure.Root, ns, func(ctx context.Context, addr hash.Hash) error {
			numaddresses++
			return nil
		})
		require.NoError(t, err)
		assert.Less(t, 4096, numaddresses)
	})

	t.Run("WalkNodes", func(t *testing.T) {
		cc, err := NewEmptyCommitClosure(ns)
		require.NoError(t, err)
		e := cc.Editor()
		for i := 0; i < 4096; i++ {
			addr, err := ns.Write(ctx, tree.NewEmptyTestNode())
			require.NoError(t, err)
			err = e.Add(ctx, NewCommitClosureKey(ns.Pool(), uint64(i), addr))
			require.NoError(t, err)
		}
		cc, err = e.Flush(ctx)
		require.NoError(t, err)
		ccc, err := cc.Count()
		require.NoError(t, err)
		assert.Equal(t, 4096, ccc)

		numnodes := 0
		totalentries := 0
		err = tree.WalkNodes(ctx, cc.closure.Root, ns, func(ctx context.Context, node tree.Node) error {
			numnodes++
			totalentries += node.Count()
			return nil
		})
		require.NoError(t, err)
		assert.Less(t, cc.closure.Root.Count(), numnodes)
		assert.Less(t, 4096, totalentries)
	})
}
