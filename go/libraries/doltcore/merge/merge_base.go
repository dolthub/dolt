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

package merge

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

	"github.com/dolthub/dolt/go/store/hash"
)

// MergeBase returns the best common ancestor of |left| and |right|. When there are several, it returns the one with the
// newest committer date, as `git merge-base` does.
func MergeBase(ctx context.Context, left, right *doltdb.Commit) (base hash.Hash, err error) {
	bases, err := MergeBases(ctx, left, right)
	if err != nil {
		return base, err
	}
	return bases[0], nil
}

// MergeBases returns every best common ancestor of |left| and |right|, newest committer date first, the same list as
// `git merge-base --all`.
func MergeBases(ctx context.Context, left, right *doltdb.Commit) ([]hash.Hash, error) {
	optCmts, err := doltdb.GetCommitAncestors(ctx, left, right)
	if err != nil {
		return nil, err
	}
	commits := make([]*doltdb.Commit, len(optCmts))
	for i, optCmt := range optCmts {
		c, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		commits[i] = c
	}
	if err = sortNewestFirst(ctx, commits); err != nil {
		return nil, err
	}
	bases := make([]hash.Hash, len(commits))
	for i, c := range commits {
		if bases[i], err = c.HashOf(); err != nil {
			return nil, err
		}
	}
	return bases, nil
}
