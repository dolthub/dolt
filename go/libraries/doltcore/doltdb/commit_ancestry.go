// Copyright 2026 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
)

// CommitAncestry tells which commits are a commit or its ancestors, from the parent closure stored with the commit.
type CommitAncestry struct {
	hash    hash.Hash
	height  uint64
	closure prolly.CommitClosure
}

// GetAncestry loads the parent closure of |c|.
func (c *Commit) GetAncestry(ctx context.Context) (*CommitAncestry, error) {
	h, err := c.HashOf()
	if err != nil {
		return nil, err
	}
	height, err := c.Height()
	if err != nil {
		return nil, err
	}
	closure, err := c.GetCommitClosure(ctx)
	if err != nil {
		return nil, err
	}
	return &CommitAncestry{hash: h, height: height, closure: closure}, nil
}

// Contains reports whether the commit |h| at |height| is this commit or one of its ancestors.
func (a *CommitAncestry) Contains(ctx context.Context, h hash.Hash, height uint64) (bool, error) {
	// Ancestors are shorter. A commit without parents has an empty closure.
	if height >= a.height {
		return h == a.hash, nil
	}
	return a.closure.ContainsKey(ctx, h, height)
}

// HeightOf returns the height of |h|, which must be an ancestor.
func (a *CommitAncestry) HeightOf(ctx context.Context, h hash.Hash) (uint64, error) {
	for height := a.height - 1; height > 0; height-- {
		if ok, err := a.closure.ContainsKey(ctx, h, height); err != nil || ok {
			return height, err
		}
	}
	return 0, fmt.Errorf("commit %s is not an ancestor of %s", h, a.hash)
}

// IterHeight iterates over the ancestors at |height|.
func (a *CommitAncestry) IterHeight(ctx context.Context, height uint64) (prolly.CommitClosureIter, error) {
	return a.closure.IterHeight(ctx, height)
}
