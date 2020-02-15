// Copyright 2020 Liquidata, Inc.
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

package rebase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	ndiff "github.com/liquidata-inc/dolt/go/store/diff"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type visitedSet map[hash.Hash]*doltdb.Commit

// replaces all instances of oldTag with newTag.
func TagRebase(ctx context.Context, dRef ref.DoltRef, ddb *doltdb.DoltDB, oldTag, newTag uint64) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec("head", dRef.String())

	if err != nil {
		return nil, err
	}

	cm, err := ddb.Resolve(ctx, cs)

	if err != nil {
		return nil, err
	}

	found, err := tagExistsInHistory(ctx, ddb, cm, oldTag)

	if !found {
		ch, _ := cm.HashOf()
		return nil, errors.New(fmt.Sprintf("tag: %d not found in commit history for commit: %s", oldTag, ch))
	}

	rebasedCommit, err := tagRebaseRecursive(ctx, ddb, cm, make(visitedSet), oldTag, newTag)

	if err != nil {
		return nil, err
	}

	err = ddb.DeleteBranch(ctx, dRef)

	if err != nil {
		return nil, err
	}

	err = ddb.NewBranchAtCommit(ctx, dRef, rebasedCommit)

	if err != nil {
		return nil, err
	}

	return rebasedCommit, nil
}

func tagRebaseRecursive(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, vs visitedSet, oldTag, newTag uint64) (*doltdb.Commit, error) {
	commitHash, err := commit.HashOf()
	if err != nil {
		return nil, err
	}
	visitedCommit, found := vs[commitHash]
	if found {
		// base case: reached previously rebased node
		return visitedCommit, nil
	}

	needToRebase, err := tagExistsInHistory(ctx, ddb, commit, oldTag)
	if err != nil {
		return nil, err
	}
	if !needToRebase {
		// base case: reached bottom of DFS,
		return commit, nil
	}

	allParents, err := ddb.ResolveAllParents(ctx, commit)

	if len(allParents) < 1 {
		panic(fmt.Sprintf("commit: %s has no parents", commitHash.String()))
	}

	var allRebasedParents []*doltdb.Commit
	for _, p := range allParents {
		rp, err := tagRebaseRecursive(ctx, ddb, p, vs, oldTag, newTag)

		if err != nil {
			return nil, err
		}

		allRebasedParents = append(allRebasedParents, rp)
	}

	root, err := commit.GetRootValue()

	if err != nil {
		return nil, err
	}

	parentRoot, err := allParents[0].GetRootValue()

	if err != nil {
		return nil, err
	}

	// we can diff off of any parent
	rebasedParentRoot, err := allRebasedParents[0].GetRootValue()

	if err != nil {
		return nil, err
	}

	rebasedRoot, err := replayCommitWithNewTag(ctx, root, parentRoot, rebasedParentRoot, oldTag, newTag)

	if err != nil {
		return nil, err
	}

	valueHash, err := ddb.WriteRootValue(ctx, rebasedRoot)

	if err != nil {
		return nil, err
	}

	oldMeta, err := commit.GetCommitMeta()

	if err != nil {
		return nil, err
	}

	rebasedCommit, err := ddb.CommitOrphanWithParentCommits(ctx, valueHash, allRebasedParents, oldMeta)

	if err != nil {
		return nil, err
	}

	vs[commitHash] = rebasedCommit
	return rebasedCommit, nil
}

func replayCommitWithNewTag(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue, oldTag, newTag uint64) (*doltdb.RootValue, error) {

	tblName, tbl, err := tableFromRootAndTag(ctx, root, oldTag)

	if err != nil {
		return nil, err
	}

	if tbl == nil {
		// tag doesn't exist in this commit
		return root, nil
	}

	parentTblName := tblName

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	// schema rebase
	var isPkTag bool
	newCC, _ := schema.NewColCollection()
	err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if tag == oldTag {
			col = schema.Column{
				Name:        col.Name,
				Tag:         newTag,
				Kind:        col.Kind,
				IsPartOfPK:  col.IsPartOfPK,
				Constraints: col.Constraints,
			}
			isPkTag = col.IsPartOfPK
		}
		newCC, err = newCC.Append(col)
		if err != nil {
			return true, err
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	rebasedSch := schema.SchemaFromCols(newCC)

	// row rebase
	var parentRows types.Map
	parentTbl, found, err := parentRoot.GetTable(ctx, tblName)
	if found && parentTbl != nil {
		parentRows, err = parentTbl.GetRowData(ctx)
	} else {
		// TODO: this could also be a renamed table
		parentRows, err = types.NewMap(ctx, parentRoot.VRW())
	}

	if err != nil {
		return nil, err
	}

	var rebasedParentRows types.Map
	rebasedParentTbl, found, err := rebasedParentRoot.GetTable(ctx, parentTblName)
	if found && rebasedParentTbl != nil {
		rebasedParentRows, err = rebasedParentTbl.GetRowData(ctx)
	} else {
		// TODO: this could also be a renamed table
		rebasedParentRows, err = types.NewMap(ctx, rebasedParentRoot.VRW())
	}

	if err != nil {
		return nil, err
	}

	rows, err := tbl.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	rebasedRows, err := replayRowDiffs(ctx, rebasedSch, rows, parentRows, rebasedParentRows, oldTag, newTag, isPkTag)

	if err != nil {
		return nil, err
	}

	rebasedSchVal, err := encoding.MarshalAsNomsValue(ctx, rebasedParentRoot.VRW(), rebasedSch)

	if err != nil {
		return nil, err
	}

	rebasedTable, err := doltdb.NewTable(ctx, rebasedParentRoot.VRW(), rebasedSchVal, rebasedRows)

	if err != nil {
		return nil, err
	}

	// create new RootValue by overwriting table with rebased rows and schema
	return doltdb.PutTable(ctx, root, root.VRW(), tblName, rebasedTable)
}

func tableFromRootAndTag(ctx context.Context, root *doltdb.RootValue, tag uint64) (string, *doltdb.Table, error) {
	tblNames, err := root.GetTableNames(ctx)

	if err != nil {
		return "", nil, err
	}

	for _, tn := range tblNames {
		t, _, _ := root.GetTable(ctx, tn)

		found, err := tagExistsInTable(ctx, t, tag)

		if found {
			return tn, t, err
		}

		if err != nil {
			return "", nil, err
		}
	}

	// tag doesn't exist in this commit
	return "", nil, nil
}

func replayRowDiffs(ctx context.Context, rSch schema.Schema, rows, parentRows, rebasedParentRows types.Map, oldTag, newTag uint64, pkTag bool) (types.Map, error) {

	rebasedTags := rSch.GetAllCols().Tags
	rebasedNBF := rows.Format()
	// we will apply modified differences to the rebasedParent
	rebasedRowEditor := rebasedParentRows.Edit()

	ad := diff.NewAsyncDiffer(1024)
	// get all differences (including merges) between original commit and its parent
	ad.Start(ctx, rows, parentRows)
	defer ad.Close()

	for {
		diffs, err := ad.GetDiffs(1, time.Second)

		if ad.IsDone() {
			break
		}

		if err != nil {
			return types.EmptyMap, err
		}

		if len(diffs) == 0 {
			return types.EmptyMap, errors.New("async diff timeout")
		}

		if len(diffs) != 1 {
			panic("only a single diff requested, multiple returned.  bug in AsyncDiffer")
		}

		d := diffs[0]
		if d.KeyValue == nil {
			panic("lol, wut")
		}

		key, newVal, err := modifyDifferenceTag(d, oldTag, newTag, pkTag, rebasedNBF, rebasedTags)

		if err != nil {
			return types.EmptyMap, nil
		}

		if d.OldValue != nil && d.NewValue != nil { // update
			rebasedRowEditor.Set(key, newVal)
		} else if d.OldValue == nil { // insert
			rebasedRowEditor.Set(key, newVal)
		} else if d.NewValue == nil { // delete
			rebasedRowEditor.Remove(key)
		} else {
			panic("bad diff")
		}
	}

	return rebasedRowEditor.Map(ctx)
}

func modifyDifferenceTag(d *ndiff.Difference, old, new uint64, pkTag bool, nbf *types.NomsBinFormat, tags []uint64) (key types.LesserValuable, val types.Valuable, err error) {
	if pkTag {
		tv, err := row.ParseTaggedValues(d.KeyValue.(types.Tuple))

		if err != nil {
			return nil, nil, err
		}

		tv[new] = tv[old]
		delete(tv, old)

		return tv.NomsTupleForTags(nbf, tags, true), d.NewValue, nil
	} else if d.NewValue != nil {
		tv, err := row.ParseTaggedValues(d.NewValue.(types.Tuple))

		if err != nil {
			return nil, nil, err
		}

		tv[new] = tv[old]
		delete(tv, old)

		return d.KeyValue, tv.NomsTupleForTags(nbf, tags, false), nil
	}
	return d.KeyValue, d.NewValue, nil
}

// TODO: replace this traversal with a check of SuperSchema once we have it
func tagExistsInHistory(ctx context.Context, ddb *doltdb.DoltDB, c *doltdb.Commit, tag uint64) (bool, error) {

	found, err := tagExistsInCommit(ctx, c, tag)

	if found {
		return found, nil
	}

	// DSF of parents
	allParents, err := ddb.ResolveAllParents(ctx, c)

	if err != nil || len(allParents) < 1 {
		return false, err
	}

	for _, pc := range allParents {

		found, err := tagExistsInHistory(ctx, ddb, pc, tag)

		if err != nil {
			return false, err
		}

		if found {
			return found, nil
		}
	}
	return false, nil
}

func tagExistsInCommit(ctx context.Context, c *doltdb.Commit, tag uint64) (bool, error) {
	root, err := c.GetRootValue()

	if err != nil {
		return false, err
	}

	tblNames, err := root.GetTableNames(ctx)

	if err != nil {
		return false, nil
	}

	for _, tn := range tblNames {
		t, _, _ := root.GetTable(ctx, tn)

		found, err := tagExistsInTable(ctx, t, tag)

		if found || err != nil {
			return found, err
		}
	}
	return false, nil
}

func tagExistsInTable(ctx context.Context, t *doltdb.Table, tag uint64) (bool, error) {
	sch, err := t.GetSchema(ctx)

	if err != nil {
		return false, err
	}

	_, found := sch.GetAllCols().GetByTag(tag)
	return found, nil
}
