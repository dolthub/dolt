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
	"bytes"
	"context"
	"encoding/binary"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type CommitClosureValue []byte

type CommitClosure struct {
	closure orderedTree[CommitClosureKey, CommitClosureValue, commitClosureKeyOrdering]
}

type commitClosureKeyOrdering struct{}

var _ ordering[CommitClosureKey] = commitClosureKeyOrdering{}

func (o commitClosureKeyOrdering) Compare(left, right CommitClosureKey) int {
	lh, rh := left.Height(), right.Height()
	if lh == rh {
		return bytes.Compare(left[8:], right[8:])
	} else if lh < rh {
		return -1
	}
	return 1
}

func NewEmptyCommitClosure(ns tree.NodeStore) CommitClosure {
	serializer := message.CommitClosureSerializer{Pool: ns.Pool()}
	msg := serializer.Serialize(nil, nil, nil, 0)
	node := tree.NodeFromBytes(msg)
	return NewCommitClosure(node, ns)
}

func NewCommitClosure(node tree.Node, ns tree.NodeStore) CommitClosure {
	return CommitClosure{
		closure: orderedTree[CommitClosureKey, CommitClosureValue, commitClosureKeyOrdering]{
			root:  node,
			ns:    ns,
			order: commitClosureKeyOrdering{},
		},
	}
}

func (c CommitClosure) Count() int {
	return c.closure.count()
}

func (c CommitClosure) Height() int {
	return c.closure.height()
}

func (c CommitClosure) Node() tree.Node {
	return c.closure.root
}

func (c CommitClosure) HashOf() hash.Hash {
	return c.closure.hashOf()
}

func (c CommitClosure) Format() *types.NomsBinFormat {
	return c.closure.ns.Format()
}

func (c CommitClosure) Editor() CommitClosureEditor {
	return CommitClosureEditor{
		closure: c.closure.mutate(),
	}
}

func (c CommitClosure) IterAllReverse(ctx context.Context) (CommitClosureIter, error) {
	return c.closure.iterAllReverse(ctx)
}

func DecodeCommitClosureKey(key []byte) (height uint64, addr hash.Hash) {
	height = binary.LittleEndian.Uint64(key)
	addr = hash.New(key[8:])
	return
}

type CommitClosureEditor struct {
	closure orderedMap[CommitClosureKey, CommitClosureValue, commitClosureKeyOrdering]
}

type CommitClosureKey []byte

type CommitClosureIter kvIter[CommitClosureKey, CommitClosureValue]

func NewCommitClosureKey(p pool.BuffPool, height uint64, addr hash.Hash) CommitClosureKey {
	r := p.Get(8 + 20)
	binary.LittleEndian.PutUint64(r, height)
	copy(r[8:], addr[:])
	return CommitClosureKey(r)
}

func (k CommitClosureKey) Height() uint64 {
	return binary.LittleEndian.Uint64(k)
}

func (k CommitClosureKey) Addr() hash.Hash {
	return hash.New(k[8:])
}

func (k CommitClosureKey) Less(other CommitClosureKey) bool {
	return commitClosureKeyOrdering{}.Compare(k, other) < 0
}

var emptyCommitClosureValue CommitClosureValue = CommitClosureValue(make([]byte, 1))

func (wr CommitClosureEditor) Add(ctx context.Context, key CommitClosureKey) error {
	return wr.closure.put(ctx, key, emptyCommitClosureValue)
}

func (wr CommitClosureEditor) Delete(ctx context.Context, key CommitClosureKey) error {
	return wr.closure.delete(ctx, key)
}

func (wr CommitClosureEditor) Flush(ctx context.Context) (CommitClosure, error) {
	tr := wr.closure.tree
	serializer := message.CommitClosureSerializer{Pool: tr.ns.Pool()}

	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, serializer, wr.closure.mutations(), tr.compareItems)
	if err != nil {
		return CommitClosure{}, err
	}

	return CommitClosure{
		closure: orderedTree[CommitClosureKey, CommitClosureValue, commitClosureKeyOrdering]{
			root:  root,
			ns:    tr.ns,
			order: tr.order,
		},
	}, nil
}

func DiffCommitClosures(ctx context.Context, from, to CommitClosure, cb DiffFn) error {
	return diffOrderedTrees(ctx, from.closure, to.closure, cb)
}
