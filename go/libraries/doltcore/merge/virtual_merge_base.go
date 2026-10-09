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

package merge

import (
	"sort"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
)

const (
	virtualMergeBaseName        = "Dolt"
	virtualMergeBaseEmail       = "virtual-merge-base@dolthub.com"
	virtualMergeBaseDescription = "virtual merge base"
)

// ResolveMergeBase returns the commit to use as the base when merging |left| and |right|. When they have several best
// common ancestors, it returns a virtual merge base built by merging those ancestors together, as git's merge-ort
// does. The virtual merge base is a dangling commit written to |ddb| whose parents are the merged ancestors. It
// depends only on |left| and |right|, so every caller gets the same commit.
func ResolveMergeBase(ctx *sql.Context, ddb *doltdb.DoltDB, left, right *doltdb.Commit) (*doltdb.Commit, error) {
	optCmts, err := doltdb.GetCommitAncestors(ctx, left, right)
	if err != nil {
		return nil, err
	}
	bases := make([]*doltdb.Commit, len(optCmts))
	for i, optCmt := range optCmts {
		base, ok := optCmt.ToCommit()
		if !ok {
			return nil, doltdb.ErrGhostCommitRuntimeFailure
		}
		bases[i] = base
	}
	if len(bases) == 1 {
		return bases[0], nil
	}

	if err = sortOldestFirst(ctx, bases); err != nil {
		return nil, err
	}
	virtualBase := bases[0]
	for _, base := range bases[1:] {
		virtualBase, err = mergeIntoVirtualBase(ctx, ddb, virtualBase, base)
		if err != nil {
			return nil, err
		}
	}
	return virtualBase, nil
}

// mergeIntoVirtualBase merges |x| and |y| into a dangling commit. A table that conflicts or violates constraints in
// this merge is taken unchanged from the merge base of |x| and |y|, so the outer merge reports the disagreement
// instead of hiding it. git's merge-ort likewise keeps the base version of content it cannot merge here.
func mergeIntoVirtualBase(ctx *sql.Context, ddb *doltdb.DoltDB, x, y *doltdb.Commit) (*doltdb.Commit, error) {
	base, err := ResolveMergeBase(ctx, ddb, x, y)
	if err != nil {
		return nil, err
	}
	xRoot, err := x.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}
	yRoot, err := y.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}
	baseRoot, err := base.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	result, err := MergeRoots(ctx, doltdb.SimpleTableResolver{}, xRoot, yRoot, baseRoot, y, base, editor.Options{}, MergeOpts{KeepSchemaConflicts: true})
	if err != nil {
		return nil, err
	}
	mergedRoot, err := takeUnmergedTablesFromBase(ctx, result, baseRoot)
	if err != nil {
		return nil, err
	}
	_, mergedRootHash, err := ddb.WriteRootValue(ctx, mergedRoot)
	if err != nil {
		return nil, err
	}

	meta, err := virtualMergeBaseMeta(ctx, x, y)
	if err != nil {
		return nil, err
	}
	return ddb.CommitDanglingWithParentCommits(ctx, mergedRootHash, []*doltdb.Commit{x, y}, meta)
}

func takeUnmergedTablesFromBase(ctx *sql.Context, result *Result, baseRoot doltdb.RootValue) (doltdb.RootValue, error) {
	unmerged := make(map[doltdb.TableName]struct{})
	for name, stats := range result.Stats {
		if stats.HasArtifacts() {
			unmerged[name] = struct{}{}
		}
	}
	for _, name := range SchemaConflictTableNames(result.SchemaConflicts) {
		unmerged[name] = struct{}{}
	}

	root := result.Root
	for name := range unmerged {
		baseTable, ok, err := baseRoot.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if ok {
			root, err = root.PutTable(ctx, name, baseTable)
		} else {
			root, err = root.RemoveTables(ctx, true, true, name)
		}
		if err != nil {
			return nil, err
		}
	}
	return root, nil
}

// virtualMergeBaseMeta derives every field from |x| and |y|, so merging the same commits always yields the same hash.
func virtualMergeBaseMeta(ctx *sql.Context, x, y *doltdb.Commit) (*datas.CommitMeta, error) {
	xDate, err := committerDate(ctx, x)
	if err != nil {
		return nil, err
	}
	yDate, err := committerDate(ctx, y)
	if err != nil {
		return nil, err
	}
	if yDate.After(xDate) {
		xDate = yDate
	}
	ident := datas.CommitIdent{Name: virtualMergeBaseName, Email: virtualMergeBaseEmail, Date: datas.CommitDateAt(xDate)}
	return datas.NewCommitMetaWithAuthorCommitter(ident, ident, virtualMergeBaseDescription)
}

// sortOldestFirst orders |commits| by committer date, oldest first, breaking ties by hash.
func sortOldestFirst(ctx *sql.Context, commits []*doltdb.Commit) error {
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
			return dated[i].date.Before(dated[j].date)
		}
		return dated[i].hash < dated[j].hash
	})
	for i := range dated {
		commits[i] = dated[i].commit
	}
	return nil
}

func committerDate(ctx *sql.Context, c *doltdb.Commit) (time.Time, error) {
	meta, err := c.GetCommitMeta(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(int64(meta.TimestampMillis())), nil
}
