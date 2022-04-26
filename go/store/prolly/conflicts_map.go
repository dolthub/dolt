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
	"context"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type Conflict val.Triple[val.Tuple]

func (c Conflict) OurValue() val.Tuple {
	return val.Tuple(val.Triple(c).First())
}

func (c Conflict) TheirValue() val.Tuple {
	return val.Tuple(val.Triple(c).Second())
}

func (c Conflict) BaseValue() val.Tuple {
	return val.Tuple(val.Triple(c).Third())
}

type ConflictIter kvIter[val.Tuple, Conflict]

type ConflictMap struct {
	conflicts orderedTree[val.Tuple, Conflict, val.TupleDesc]
	keyDesc   val.TupleDesc
	ourDesc   val.TupleDesc
	theirDesc val.TupleDesc
	baseDesc  val.TupleDesc
}

func NewConflictMap(ns tree.NodeStore, key, ours, theirs, base val.TupleDesc) ConflictMap {
	conflicts := orderedTree[val.Tuple, Conflict, val.TupleDesc]{
		root:  tree.NewEmptyNode(ns.Pool()),
		ns:    ns,
		order: key,
	}
	return ConflictMap{
		conflicts: conflicts,
		keyDesc:   key,
		ourDesc:   ours,
		theirDesc: theirs,
		baseDesc:  base,
	}
}

// Count returns the number of key-value pairs in the Map.
func (c ConflictMap) Count() int {
	return c.conflicts.count()
}

func (c ConflictMap) Height() int {
	return c.conflicts.height()
}

// HashOf returns the Hash of this Map.
func (c ConflictMap) HashOf() hash.Hash {
	return c.conflicts.hashOf()
}

// Format returns the NomsBinFormat of this Map.
func (c ConflictMap) Format() *types.NomsBinFormat {
	return c.conflicts.ns.Format()
}

// Descriptors returns the TupleDesc's from this Map.
func (c ConflictMap) Descriptors() (key, ours, theirs, base val.TupleDesc) {
	return c.keyDesc, c.ourDesc, c.theirDesc, c.baseDesc
}

func (c ConflictMap) WalkAddresses(ctx context.Context, cb tree.AddressCb) error {
	return c.conflicts.walkAddresses(ctx, cb)
}

func (c ConflictMap) WalkNodes(ctx context.Context, cb tree.NodeCb) error {
	return c.conflicts.walkNodes(ctx, cb)
}

func (c ConflictMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn[val.Tuple, Conflict]) (err error) {
	return c.conflicts.get(ctx, key, cb)
}

func (c ConflictMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	return c.conflicts.has(ctx, key)
}

// IterAll returns a mutableMapIter that iterates over the entire Map.
func (c ConflictMap) IterAll(ctx context.Context) (ConflictIter, error) {
	return c.conflicts.iterAll(ctx)
}

// IterOrdinalRange returns a MapIter for the ordinal range beginning at |start| and ending before |stop|.
func (c ConflictMap) IterOrdinalRange(ctx context.Context, start, stop uint64) (ConflictIter, error) {
	return c.conflicts.iterOrdinalRange(ctx, start, stop)
}

func (c ConflictMap) Editor() ConflictEditor {
	return ConflictEditor{
		conflicts: c.conflicts.mutate(),
		keyDesc:   c.keyDesc,
		ourDesc:   c.ourDesc,
		theirDesc: c.theirDesc,
		baseDesc:  c.baseDesc,
	}
}

type ConflictEditor struct {
	conflicts orderedMap[val.Tuple, Conflict, val.TupleDesc]
	keyDesc   val.TupleDesc
	ourDesc   val.TupleDesc
	theirDesc val.TupleDesc
	baseDesc  val.TupleDesc
}

func (wr ConflictEditor) Add(ctx context.Context, key, ourVal, theirVal, baseVal val.Tuple) error {
	p := wr.conflicts.tree.ns.Pool()
	c := val.NewTriple(p, ourVal, theirVal, baseVal)
	return wr.conflicts.put(ctx, key, c)
}

func (wr ConflictEditor) Delete(ctx context.Context, key val.Tuple) error {
	return wr.conflicts.delete(ctx, key)
}

func (wr ConflictEditor) Flush(ctx context.Context) (ConflictMap, error) {
	root, err := wr.conflicts.makeTree(ctx)
	if err != nil {
		return ConflictMap{}, err
	}
	return ConflictMap{
		conflicts: root,
		keyDesc:   wr.keyDesc,
		ourDesc:   wr.ourDesc,
		theirDesc: wr.theirDesc,
		baseDesc:  wr.baseDesc,
	}, nil
}
