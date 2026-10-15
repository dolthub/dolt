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
	"sort"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

	"github.com/dolthub/dolt/go/store/hash"
)

func MergeBase(ctx context.Context, left, right *doltdb.Commit) (base hash.Hash, err error) {
	optCmt, err := doltdb.GetCommitAncestor(ctx, left, right)
	if err != nil {
		return base, err
	}
	ancestor, ok := optCmt.ToCommit()
	if !ok {
		return base, doltdb.ErrGhostCommitEncountered
	}

	return ancestor.HashOf()
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
	return sortNewestFirst(ctx, commits)
}

// sortNewestFirst returns the hashes of |commits| ordered by committer date, newest first, breaking ties by hash. git
// orders merge bases the same way (commit_list_insert_by_date in commit-reach.c). Checked with git 2.39: in one history,
// swapping the committer dates of two merge bases swaps their order in `git merge-base --all`, regardless of their
// heights, and author dates do not affect it. git leaves commits with equal dates in no defined order.
func sortNewestFirst(ctx context.Context, commits []*doltdb.Commit) ([]hash.Hash, error) {
	type datedCommit struct {
		date time.Time
		hash hash.Hash
	}
	dated := make([]datedCommit, len(commits))
	for i, c := range commits {
		meta, err := c.GetCommitMeta(ctx)
		if err != nil {
			return nil, err
		}
		h, err := c.HashOf()
		if err != nil {
			return nil, err
		}
		dated[i] = datedCommit{date: time.UnixMilli(int64(meta.TimestampMillis())), hash: h}
	}
	sort.Slice(dated, func(i, j int) bool {
		if !dated[i].date.Equal(dated[j].date) {
			return dated[i].date.After(dated[j].date)
		}
		return dated[i].hash.String() < dated[j].hash.String()
	})
	hashes := make([]hash.Hash, len(dated))
	for i := range dated {
		hashes[i] = dated[i].hash
	}
	return hashes, nil
}
