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
	if err = sortByCommitterDate(ctx, commits, true); err != nil {
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

// sortByCommitterDate orders |commits| by committer date, newest or oldest first, breaking ties by hash. git orders
// merge bases newest first (commit_list_insert_by_date in commit-reach.c). Checked with git 2.39: in one history,
// swapping the committer dates of two merge bases swaps their order in `git merge-base --all`, regardless of their
// heights, and author dates do not affect it. git leaves commits with equal dates in no defined order.
func sortByCommitterDate(ctx context.Context, commits []*doltdb.Commit, newestFirst bool) error {
	type datedCommit struct {
		commit *doltdb.Commit
		date   time.Time
		hash   string
	}
	dated := make([]datedCommit, len(commits))
	for i, c := range commits {
		date, err := committerDate(ctx, c)
		if err != nil {
			return err
		}
		h, err := c.HashOf()
		if err != nil {
			return err
		}
		dated[i] = datedCommit{commit: c, date: date, hash: h.String()}
	}
	sort.Slice(dated, func(i, j int) bool {
		if !dated[i].date.Equal(dated[j].date) {
			return dated[i].date.Before(dated[j].date) != newestFirst
		}
		return dated[i].hash < dated[j].hash
	})
	for i := range dated {
		commits[i] = dated[i].commit
	}
	return nil
}

func committerDate(ctx context.Context, c *doltdb.Commit) (time.Time, error) {
	meta, err := c.GetCommitMeta(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(int64(meta.TimestampMillis())), nil
}
