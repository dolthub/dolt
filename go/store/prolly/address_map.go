// Copyright 2021 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type AddressMap struct {
	addresses orderedTree[stringSlice, address, lexicographic]
}

func NewEmptyAddressMap(ns tree.NodeStore) AddressMap {
	return AddressMap{
		addresses: orderedTree[stringSlice, address, lexicographic]{
			root:  newEmptyMapNode(ns.Pool()),
			ns:    ns,
			order: lexicographic{},
		},
	}
}

type stringSlice []byte

type address []byte

type lexicographic struct{}

var _ ordering[stringSlice] = lexicographic{}

func (l lexicographic) Compare(left, right stringSlice) int {
	return bytes.Compare(left, right)
}

func (c AddressMap) Count() int {
	return c.addresses.count()
}

func (c AddressMap) Height() int {
	return c.addresses.height()
}

func (c AddressMap) HashOf() hash.Hash {
	return c.addresses.hashOf()
}

func (c AddressMap) Format() *types.NomsBinFormat {
	return c.addresses.ns.Format()
}

func (c AddressMap) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return c.addresses.walkAddresses(ctx, cb)
}

func (c AddressMap) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return c.addresses.walkNodes(ctx, cb)
}

func (c AddressMap) Get(ctx context.Context, name string) (addr hash.Hash, err error) {
	err = c.addresses.get(ctx, stringSlice(name), func(n stringSlice, a address) error {
		if n != nil {
			addr = hash.New(a)
		}
		return nil
	})
	return
}

func (c AddressMap) Has(ctx context.Context, name string) (ok bool, err error) {
	return c.addresses.has(ctx, stringSlice(name))
}

func (c AddressMap) IterAll(ctx context.Context, cb func(name string, address hash.Hash) error) error {
	iter, err := c.addresses.iterAll(ctx)
	if err != nil {
		return err
	}

	var n stringSlice
	var a address
	for {
		n, a, err = iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err = cb(string(n), hash.New(a)); err != nil {
			return err
		}
	}
	return nil
}

func (c AddressMap) Editor() AddressMapEditor {
	return AddressMapEditor{
		addresses: c.addresses.mutate(),
	}
}

type AddressMapEditor struct {
	addresses orderedMap[stringSlice, address, lexicographic]
}

func (wr AddressMapEditor) Add(ctx context.Context, name string, addr hash.Hash) error {
	return wr.addresses.put(ctx, stringSlice(name), addr[:])
}

func (wr AddressMapEditor) Update(ctx context.Context, name string, addr hash.Hash) error {
	return wr.addresses.put(ctx, stringSlice(name), addr[:])
}

func (wr AddressMapEditor) Delete(ctx context.Context, name string) error {
	return wr.addresses.delete(ctx, stringSlice(name))
}

func (wr AddressMapEditor) Flush(ctx context.Context) (AddressMap, error) {
	factory := newAddrMapBuilder
	tr := wr.addresses.tree

	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, factory, wr.addresses.mutations(), tr.compareItems)
	if err != nil {
		return AddressMap{}, err
	}

	return AddressMap{
		addresses: orderedTree[stringSlice, address, lexicographic]{
			root:  root,
			ns:    tr.ns,
			order: tr.order,
		},
	}, nil
}

func newAddrMapBuilder(level int) *addrMapBuilder {
	return &addrMapBuilder{level: level}
}

var _ tree.NodeBuilderFactory[*addrMapBuilder] = newAddrMapBuilder

type addrMapBuilder struct {
	keys, values [][]byte
	size, level  int

	subtrees tree.SubtreeCounts
}

var _ tree.NodeBuilder = &addrMapBuilder{}

func (nb *addrMapBuilder) StartNode() {
	nb.reset()
}

func (nb *addrMapBuilder) HasCapacity(key, value tree.Item) bool {
	sum := nb.size + len(key) + len(value)
	return sum <= int(message.MaxVectorOffset)
}

func (nb *addrMapBuilder) AddItems(key, value tree.Item, subtree uint64) {
	nb.keys = append(nb.keys, key)
	nb.values = append(nb.values, value)
	nb.size += len(key) + len(value)
	nb.subtrees = append(nb.subtrees, subtree)
}

func (nb *addrMapBuilder) Count() int {
	return len(nb.keys)
}

func (nb *addrMapBuilder) reset() {
	// buffers are copied, it's safe to re-use the memory.
	nb.keys = nb.keys[:0]
	nb.values = nb.values[:0]
	nb.size = 0
	nb.subtrees = nb.subtrees[:0]
}

func (nb *addrMapBuilder) Build(pool pool.BuffPool) (node tree.Node) {
	msg := message.SerializeAddressMap(pool, nb.keys, nb.values, nb.level, nb.subtrees)
	nb.reset()
	return tree.NodeFromBytes(msg)
}
