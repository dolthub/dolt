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
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type AddressMap struct {
	addresses tree.StaticMap[stringSlice, address, lexicographic]
}

func NewEmptyAddressMap(ns tree.NodeStore) (AddressMap, error) {
	serializer := message.NewAddressMapSerializer(ns.Pool())
	msg := serializer.Serialize(nil, nil, nil, 0)
	n, _, err := tree.NodeFromBytes(msg)
	if err != nil {
		return AddressMap{}, err
	}
	return NewAddressMap(n, ns)
}

func NewAddressMap(node tree.Node, ns tree.NodeStore) (AddressMap, error) {
	return AddressMap{
		addresses: tree.StaticMap[stringSlice, address, lexicographic]{
			Root:      node,
			NodeStore: ns,
			Order:     lexicographic{},
		},
	}, nil
}

type stringSlice []byte

type address []byte

type lexicographic struct{}

var _ tree.Ordering[stringSlice] = lexicographic{}

func (l lexicographic) Compare(left, right stringSlice) int {
	return bytes.Compare(left, right)
}

func (c AddressMap) Count() (int, error) {
	return c.addresses.Count()
}

func (c AddressMap) Height() int {
	return c.addresses.Height()
}

func (c AddressMap) Node() tree.Node {
	return c.addresses.Root
}

func (c AddressMap) HashOf() hash.Hash {
	return c.addresses.HashOf()
}

func (c AddressMap) Format() *types.NomsBinFormat {
	return c.addresses.NodeStore.Format()
}

func (c AddressMap) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return c.addresses.WalkAddresses(ctx, cb)
}

func (c AddressMap) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return c.addresses.WalkNodes(ctx, cb)
}

func (c AddressMap) Get(ctx context.Context, name string) (addr hash.Hash, err error) {
	err = c.addresses.Get(ctx, stringSlice(name), func(n stringSlice, a address) error {
		if n != nil {
			addr = hash.New(a)
		}
		return nil
	})
	return
}

func (c AddressMap) Has(ctx context.Context, name string) (ok bool, err error) {
	return c.addresses.Has(ctx, stringSlice(name))
}

func (c AddressMap) IterAll(ctx context.Context, cb func(name string, address hash.Hash) error) error {
	iter, err := c.addresses.IterAll(ctx)
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
		addresses: c.addresses.Mutate(),
	}
}

type AddressMapEditor struct {
	addresses tree.MutableMap[stringSlice, address, lexicographic, tree.StaticMap[stringSlice, address, lexicographic]]
}

func (wr AddressMapEditor) Add(ctx context.Context, name string, addr hash.Hash) error {
	return wr.addresses.Put(ctx, stringSlice(name), addr[:])
}

func (wr AddressMapEditor) Update(ctx context.Context, name string, addr hash.Hash) error {
	return wr.addresses.Put(ctx, stringSlice(name), addr[:])
}

func (wr AddressMapEditor) Delete(ctx context.Context, name string) error {
	return wr.addresses.Delete(ctx, stringSlice(name))
}

func (wr AddressMapEditor) Flush(ctx context.Context) (AddressMap, error) {
	sm := wr.addresses.Static
	serializer := message.NewAddressMapSerializer(sm.NodeStore.Pool())
	fn := tree.ApplyMutations[stringSlice, lexicographic, message.AddressMapSerializer]

	root, err := fn(ctx, sm.NodeStore, sm.Root, lexicographic{}, serializer, wr.addresses.Mutations())
	if err != nil {
		return AddressMap{}, err
	}

	return AddressMap{
		addresses: tree.StaticMap[stringSlice, address, lexicographic]{
			Root:      root,
			NodeStore: sm.NodeStore,
			Order:     sm.Order,
		},
	}, nil
}
