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
	"github.com/dolthub/dolt/go/store/val"
)

type metaPair struct {
	k, r      nodeItem
	treeCount uint64
}

func (p metaPair) key() val.Tuple {
	return val.Tuple(p.k)
}

func (p metaPair) ref() hash.Hash {
	return hash.New(p.r)
}

func (p metaPair) subtreeCount() uint64 {
	return p.treeCount
}

func fetchChild(ctx context.Context, ns NodeStore, ref hash.Hash) (Node, error) {
	// todo(andy) handle nil Node, dangling ref
	return ns.Read(ctx, ref)
}

func writeNewChild(ctx context.Context, ns NodeStore, level uint64, keys, values []nodeItem) (Node, metaPair, error) {
	child := buildMapNode(ns.Pool(), level, keys, values)

	ref, err := ns.Write(ctx, child)
	if err != nil {
		return Node{}, metaPair{}, err
	}

	if len(keys) == 0 {
		// empty leaf node
		return child, metaPair{}, nil
	}

	lastKey := val.Tuple(keys[len(keys)-1])
	metaKey := val.CloneTuple(ns.Pool(), lastKey)
	meta := metaPair{k: nodeItem(metaKey), r: nodeItem(ref[:])}

	return child, meta, nil
}
