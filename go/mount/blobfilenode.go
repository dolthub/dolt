// Copyright 2025 Dolthub, Inc.
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

package mount

import (
	"bazil.org/fuse"
	"context"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

type blobFileNode struct {
	ns   tree.NodeStore
	node tree.Node
}

var _ File = blobFileNode{}

func (b blobFileNode) Attr(ctx context.Context, a *fuse.Attr) (err error) {
	a.Mode = 0o444
	// TODO: Report size just from the tree
	size := uint64(0)
	err = tree.WalkNodes(ctx, b.node, b.ns, func(ctx context.Context, n tree.Node) error {
		if n.IsLeaf() {
			size += uint64(len(n.GetValue(0)))
		}
		return nil
	})
	a.Size = size
	return err
}

func (b blobFileNode) ReadAll(ctx context.Context) (result []byte, err error) {
	err = tree.WalkNodes(ctx, b.node, b.ns, func(ctx context.Context, n tree.Node) error {
		if n.IsLeaf() {
			result = append(result, n.GetValue(0)...)
		}
		return nil
	})
	return result, err
}
