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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/message"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type Conflict val.Triple[val.Tuple]

func (c Conflict) OurValue() val.Tuple {
	return val.Triple[val.Tuple](c).First()
}

func (c Conflict) TheirValue() val.Tuple {
	return val.Triple[val.Tuple](c).Second()
}

func (c Conflict) BaseValue() val.Tuple {
	return val.Triple[val.Tuple](c).Third()
}

type ConflictIter kvIter[val.Tuple, Conflict]

type ConflictMap struct {
	conflicts orderedTree[val.Tuple, Conflict, val.TupleDesc]
	keyDesc   val.TupleDesc
	ourDesc   val.TupleDesc
	theirDesc val.TupleDesc
	baseDesc  val.TupleDesc
}

func NewConflictMap(root tree.Node, ns tree.NodeStore, key, ours, theirs, base val.TupleDesc) ConflictMap {
	conflicts := orderedTree[val.Tuple, Conflict, val.TupleDesc]{
		root:  root,
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

func NewEmptyConflictMap(ns tree.NodeStore, key, ours, theirs, base val.TupleDesc) ConflictMap {
	return NewConflictMap(newEmptyMapNode(ns.Pool()), ns, key, ours, theirs, base)
}

func (c ConflictMap) Count() int {
	return c.conflicts.count()
}

func (c ConflictMap) Height() int {
	return c.conflicts.height()
}

func (c ConflictMap) HashOf() hash.Hash {
	return c.conflicts.hashOf()
}

func (c ConflictMap) Node() tree.Node {
	return c.conflicts.root
}

func (c ConflictMap) Format() *types.NomsBinFormat {
	return c.conflicts.ns.Format()
}

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

func (c ConflictMap) IterAll(ctx context.Context) (ConflictIter, error) {
	return c.conflicts.iterAll(ctx)
}

func (c ConflictMap) IterOrdinalRange(ctx context.Context, start, stop uint64) (ConflictIter, error) {
	return c.conflicts.iterOrdinalRange(ctx, start, stop)
}

// Pool returns the pool.BuffPool of the underlying conflicts' tree.NodeStore
func (c ConflictMap) Pool() pool.BuffPool {
	return c.conflicts.ns.Pool()
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
	return wr.conflicts.put(ctx, key, Conflict(c))
}

func (wr ConflictEditor) Delete(ctx context.Context, key val.Tuple) error {
	return wr.conflicts.delete(ctx, key)
}

func (wr ConflictEditor) Flush(ctx context.Context) (ConflictMap, error) {
	tr := wr.conflicts.tree
	serializer := message.ProllyMapSerializer{Pool: tr.ns.Pool()}

	root, err := tree.ApplyMutations(ctx, tr.ns, tr.root, serializer, wr.conflicts.mutations(), tr.compareItems)
	if err != nil {
		return ConflictMap{}, err
	}

	return ConflictMap{
		conflicts: orderedTree[val.Tuple, Conflict, val.TupleDesc]{
			root:  root,
			ns:    tr.ns,
			order: tr.order,
		},
		keyDesc:   wr.keyDesc,
		ourDesc:   wr.ourDesc,
		theirDesc: wr.theirDesc,
		baseDesc:  wr.baseDesc,
	}, nil
}

// ConflictDebugFormat formats a ConflictMap.
func ConflictDebugFormat(ctx context.Context, m ConflictMap) (string, error) {
	kd, ourVD, theirVD, baseVD := m.Descriptors()
	iter, err := m.IterAll(ctx)
	if err != nil {
		return "", err
	}
	c := m.Count()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Prolly Map (count: %d) {\n", c))
	for {
		k, v, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		sb.WriteString("\t")
		sb.WriteString(kd.Format(k))
		sb.WriteString(": ")
		if len(v.OurValue()) == 0 {
			sb.WriteString("NULL")
		} else {
			sb.WriteString(ourVD.Format(v.OurValue()))
		}
		sb.WriteString(", ")
		if len(v.TheirValue()) == 0 {
			sb.WriteString("NULL")
		} else {
			sb.WriteString(theirVD.Format(v.TheirValue()))
		}
		sb.WriteString(", ")
		if len(v.BaseValue()) == 0 {
			sb.WriteString("NULL")
		} else {
			sb.WriteString(baseVD.Format(v.BaseValue()))
		}
		sb.WriteString(",\n")
	}
	sb.WriteString("}")
	return sb.String(), nil
}
