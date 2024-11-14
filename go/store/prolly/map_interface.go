// Copyright 2024 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// MapInterface is a common interface for prolly-tree based maps.
type MapInterface interface {
	Node() tree.Node
	NodeStore() tree.NodeStore
	Count() (int, error)
	HashOf() hash.Hash
	WalkNodes(ctx context.Context, cb tree.NodeCb) error
	Descriptors() (val.TupleDesc, val.TupleDesc)
	IterAll(ctx context.Context) (MapIter, error)
	Pool() pool.BuffPool
	Has(ctx context.Context, key val.Tuple) (ok bool, err error)
	ValDesc() val.TupleDesc
	KeyDesc() val.TupleDesc
}

// MapInterface is a common interface for prolly-tree based maps that can be used as the basis of a mutable map that
// implements MutableMapInterface.
type MapInterfaceWithMutable interface {
	MapInterface
	MutateInterface() MutableMapInterface
}

// MutableMapInterface is a common interface for prolly-tree based maps that can be mutated.
type MutableMapInterface interface {
	NodeStore() tree.NodeStore
	Put(ctx context.Context, key, value val.Tuple) error
	Delete(ctx context.Context, key val.Tuple) error
	Checkpoint(ctx context.Context) error
	Revert(ctx context.Context)
	HasEdits() bool
	IterRange(ctx context.Context, rng Range) (MapIter, error)
	MapInterface(ctx context.Context) (MapInterface, error)
}
