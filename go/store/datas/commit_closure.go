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

package datas

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func NewParentsClosure(ctx context.Context, c *Commit, sv types.SerialMessage, vr types.ValueReader, ns tree.NodeStore) (prolly.CommitClosure, error) {
	var msg serial.Commit
	err := serial.InitCommitRoot(&msg, sv, serial.MessagePrefixSz)
	if err != nil {
		return prolly.CommitClosure{}, err
	}
	addr := hash.New(msg.ParentClosureBytes())
	if addr.IsEmpty() {
		return prolly.CommitClosure{}, nil
	}
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return prolly.CommitClosure{}, err
	}
	if types.IsNull(v) {
		return prolly.CommitClosure{}, fmt.Errorf("internal error or data loss: dangling commit parent closure for addr %s for commit %s", addr.String(), c.Addr().String())
	}
	node, fileId, err := tree.NodeFromBytes(v.(types.SerialMessage))
	if err != nil {
		return prolly.CommitClosure{}, err
	}
	if fileId != serial.CommitClosureFileID {
		return prolly.CommitClosure{}, fmt.Errorf("unexpected file ID for commit closure, expected %s, found %s", serial.CommitClosureFileID, fileId)
	}
	return prolly.NewCommitClosure(node, ns)
}

func newParentsClosureIterator(ctx context.Context, c *Commit, vr types.ValueReader, ns tree.NodeStore) (parentsClosureIter, error) {
	sv := c.NomsValue()

	sm := sv.(types.SerialMessage)
	cc, err := NewParentsClosure(ctx, c, sm, vr, ns)
	if err != nil {
		return nil, err
	}
	if cc.IsEmpty() {
		return nil, nil
	}
	ci, err := cc.IterAllReverse(ctx)
	if err != nil {
		return nil, err
	}
	return &fbParentsClosureIterator{i: ci, curr: prolly.NewCommitClosureKey(ns.Pool(), c.Height(), c.Addr()), err: nil}, nil
}

type parentsClosureIter interface {
	Err() error
	Hash() hash.Hash
	Height() uint64
	Less(ctx context.Context, nbf *types.NomsBinFormat, itr parentsClosureIter) bool
	Next(context.Context) bool
}

type fbParentsClosureIterator struct {
	i    prolly.CommitClosureIter
	err  error
	curr prolly.CommitClosureKey
}

func (i *fbParentsClosureIterator) Err() error {
	return i.err
}

func (i *fbParentsClosureIterator) Height() uint64 {
	if i.err != nil {
		return 0
	}
	return i.curr.Height()
}

func (i *fbParentsClosureIterator) Hash() hash.Hash {
	if i.err != nil {
		return hash.Hash{}
	}
	return i.curr.Addr()
}

func (i *fbParentsClosureIterator) Next(ctx context.Context) bool {
	if i.err != nil {
		return false
	}
	i.curr, _, i.err = i.i.Next(ctx)
	if i.err == io.EOF {
		i.err = nil
		return false
	}
	return true
}

func (i *fbParentsClosureIterator) Less(ctx context.Context, nbf *types.NomsBinFormat, otherI parentsClosureIter) bool {
	other := otherI.(*fbParentsClosureIterator)
	return i.curr.Less(ctx, other.curr)
}

func writeFbCommitParentClosure(ctx context.Context, cs chunks.ChunkStore, vrw types.ValueReadWriter, ns tree.NodeStore, parents []*serial.Commit, parentAddrs []hash.Hash) (hash.Hash, error) {
	if len(parents) == 0 {
		// We write an empty hash for parent-less commits of height 1.
		return hash.Hash{}, nil
	}
	// Fetch the parent closures of our parents.
	addrs := make([]hash.Hash, len(parents))
	for i := range parents {
		addrs[i] = hash.New(parents[i].ParentClosureBytes())
	}
	vs, err := vrw.ReadManyValues(ctx, addrs)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: ReadManyValues: %w", err)
	}
	// Load them as ProllyTrees.
	closures := make([]prolly.CommitClosure, len(parents))
	for i := range addrs {
		if !types.IsNull(vs[i]) {
			node, fileId, err := tree.NodeFromBytes(vs[i].(types.SerialMessage))
			if err != nil {
				return hash.Hash{}, err
			}
			if fileId != serial.CommitClosureFileID {
				return hash.Hash{}, fmt.Errorf("unexpected file ID for commit closure, expected %s, found %s", serial.CommitClosureFileID, fileId)
			}
			closures[i], err = prolly.NewCommitClosure(node, ns)
			if err != nil {
				return hash.Hash{}, err
			}
		} else {
			closures[i], err = prolly.NewEmptyCommitClosure(ns)
			if err != nil {
				return hash.Hash{}, err
			}
		}
	}
	// Add all the missing entries from [1, ...) maps to the 0th map.
	editor := closures[0].Editor()
	for i := 1; i < len(closures); i++ {
		err = prolly.DiffCommitClosures(ctx, closures[0], closures[i], func(ctx context.Context, diff tree.Diff) error {
			if diff.Type == tree.AddedDiff {
				return editor.Add(ctx, prolly.CommitClosureKey(diff.Key))
			}
			return nil
		})
		if err != nil && !errors.Is(err, io.EOF) {
			return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: DiffCommitClosures: %w", err)
		}
	}
	// Add the parents themselves to the new map.
	for i := 0; i < len(parents); i++ {
		err = editor.Add(ctx, prolly.NewCommitClosureKey(ns.Pool(), parents[i].Height(), parentAddrs[i]))
		if err != nil {
			return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: MutableCommitClosure.Put: %w", err)
		}
	}
	// This puts the closure in the NodeStore as well.
	res, err := editor.Flush(ctx)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: MutableCommitClosure.Flush: %w", err)
	}
	return res.HashOf(), nil
}
