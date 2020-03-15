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
	"sync/atomic"
	"time"

"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
ndiff "github.com/liquidata-inc/dolt/go/store/diff"
"github.com/liquidata-inc/dolt/go/store/types"
)



func MaybeMigrateUniqueTags(ctx context.Context, ddb *doltdb.DoltDB) error {
	bb, err := ddb.GetBranches(ctx)

	if err != nil {
		return err
	}

	migrate := false
	for _, b := range bb {
		cs, err := doltdb.NewCommitSpec("head", b.String())

		if err != nil {
			return err
		}

		c, err := ddb.Resolve(ctx, cs)

		if err != nil {
			return err
		}

		r, err := c.GetRootValue()

		if err != nil {
			return err
		}

		_, err = r.GetDoltVersion(ctx)

		if err == doltdb.ErrDoltVersionNotFound {
			migrate = true
		} else if err != nil {
			return err
		}
	}

	if migrate {
		err = migrateUniqueTags(ctx, ddb, bb)
	}

	return err
}

// replaces all instances of oldTag with newTag.
func TagRebaseForRef(ctx context.Context, dRef ref.DoltRef, ddb *doltdb.DoltDB, tblName string, tagMapping map[uint64]uint64) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec("head", dRef.String())

	if err != nil {
		return nil, err
	}

	cm, err := ddb.Resolve(ctx, cs)

	if err != nil {
		return nil, err
	}

	rebasedCommit, err := TagRebaseForCommit(ctx, cm, ddb, tblName, tagMapping)

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

func TagRebaseForCommit(ctx context.Context, startingCommit *doltdb.Commit, ddb *doltdb.DoltDB, tblName string, tagMapping map[uint64]uint64) (*doltdb.Commit, error) {
	err := validateTagMapping(tagMapping)

	if err != nil {
		return nil, err
	}

	found, err := tagExistsInHistory(ctx, startingCommit, tagMapping)

	if err != nil {
		return nil, err
	}

	if !found {
		ch, _ := startingCommit.HashOf()
		return nil, errors.New(fmt.Sprintf("tags not found in commit history for commit: %s", ch))
	}

	replay := func(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue) (rebaseRoot *doltdb.RootValue, err error) {
		return replayCommitWithNewTag(ctx, root, parentRoot, rebasedParentRoot, tblName, tagMapping)
	}

	nerf := func(ctx context.Context, cm *doltdb.Commit) (b bool, err error) {
		return tagExistsInHistory(ctx, cm, tagMapping)
	}

	rc, err := rebase(ctx, ddb, replay, nerf, startingCommit)

	if err != nil {
		return nil, err
	}

	return rc[0], nil
}

func replayCommitWithNewTag(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue, tblName string, tagMapping map[uint64]uint64) (*doltdb.RootValue, error) {


	tbl, found, err := root.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !found {
		return root, nil
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// tags may not exist in this commit
	tagExists := false
	for oldTag, _ := range tagMapping {
		if _, found := sch.GetAllCols().GetByTag(oldTag); found {
			tagExists = true
			break
		}
	}
	if !tagExists {
		return root, nil
	}

	parentTblName := tblName

	// schema rebase
	schCC, _ := schema.NewColCollection()
	err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if newTag, found := tagMapping[tag]; found {
			col.Tag = newTag
		}
		schCC, err = schCC.Append(col)
		if err != nil {
			return true, err
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	rebasedSch := schema.SchemaFromCols(schCC)

	// super schema rebase
	ss, _, err := root.GetSuperSchema(ctx, tblName)

	if err != nil {
		return nil, err
	}

	rebasedSS, err := ss.RebaseTag(tagMapping)

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

	rebasedRows, err := replayRowDiffs(ctx, rebasedSch, rows, parentRows, rebasedParentRows, tagMapping)

	if err != nil {
		return nil, err
	}

	rebasedSchVal, err := encoding.MarshalSchemaAsNomsValue(ctx, rebasedParentRoot.VRW(), rebasedSch)

	if err != nil {
		return nil, err
	}

	rebasedTable, err := doltdb.NewTable(ctx, rebasedParentRoot.VRW(), rebasedSchVal, rebasedRows)

	if err != nil {
		return nil, err
	}

	rebasedRoot, err := root.PutSuperSchema(ctx, tblName, rebasedSS)

	if err != nil {
		return nil, err
	}

	// create new RootValue by overwriting table with rebased rows and schema
	return doltdb.PutTable(ctx, rebasedRoot, rebasedRoot.VRW(), tblName, rebasedTable)
}

func replayRowDiffs(ctx context.Context, rSch schema.Schema, rows, parentRows, rebasedParentRows types.Map, tagMapping map[uint64]uint64) (types.Map, error) {

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

		if len(diffs) != 1 {
			panic("only a single diff requested, multiple returned.  bug in AsyncDiffer")
		}

		d := diffs[0]
		if d.KeyValue == nil {
			panic("Unexpected commit diff result: with nil key value encountered")
		}

		key, newVal, err := modifyDifferenceTag(d, rows.Format(), rSch, tagMapping)

		if err != nil {
			return types.EmptyMap, nil
		}

		switch d.ChangeType {
		case types.DiffChangeAdded:
			rebasedRowEditor.Set(key, newVal)
		case types.DiffChangeRemoved:
			rebasedRowEditor.Remove(key)
		case types.DiffChangeModified:
			rebasedRowEditor.Set(key, newVal)
		}
	}

	return rebasedRowEditor.Map(ctx)
}

func modifyDifferenceTag(d *ndiff.Difference, nbf *types.NomsBinFormat, rSch schema.Schema, tagMapping map[uint64]uint64) (key types.LesserValuable, val types.Valuable, err error) {
	ktv, err := row.ParseTaggedValues(d.KeyValue.(types.Tuple))

	if err != nil {
		return nil, nil, err
	}

	for oldTag, newTag := range tagMapping {
		if v, ok := ktv[oldTag]; ok {
			ktv[newTag] = v
			delete(ktv, oldTag)
		}
	}
	key = ktv.NomsTupleForTags(nbf, rSch.GetPKCols().Tags, true)

	val = d.NewValue
	if d.NewValue != nil {
		tv, err := row.ParseTaggedValues(d.NewValue.(types.Tuple))

		if err != nil {
			return nil, nil, err
		}

		for oldTag, newTag := range tagMapping {
			if v, ok := tv[oldTag]; ok {
				tv[newTag] = v
				delete(tv, oldTag)
			}
		}

		val = tv.NomsTupleForTags(nbf, rSch.GetNonPKCols().Tags, false)
	}

	return key, val, nil
}

func tagExistsInHistory(ctx context.Context, c *doltdb.Commit, tagMapping map[uint64]uint64) (bool, error) {

	crt, err := c.GetRootValue()

	if err != nil {
		return false, err
	}

	tblNames, err := crt.GetTableNames(ctx)

	if err != nil {
		return false, err
	}

	for _, tn := range tblNames {
		ss, _, err := crt.GetSuperSchema(ctx, tn)

		if err != nil {
			return false, err
		}

		for oldTag, _ := range tagMapping {
			if _, found := ss.GetColumn(oldTag); found {
				return true, nil
			}
		}
	}

	return false, nil
}

func validateTagMapping(tagMapping map[uint64]uint64) error {
	newTags := make(map[uint64]struct{})
	for _, nt := range tagMapping {
		if _, found := newTags[nt]; found {
			return fmt.Errorf("duplicate tag %d found in tag mapping", nt)
		}
		newTags[nt] = struct{}{}
	}
	return nil
}

func migrateUniqueTags(ctx context.Context, ddb *doltdb.DoltDB, branches []ref.DoltRef) error {
	var headCommits []*doltdb.Commit
	for _, dRef := range branches {

		cs, err := doltdb.NewCommitSpec("head", dRef.String())

		if err != nil {
			return err
		}

		cm, err := ddb.Resolve(ctx, cs)

		if err != nil {
			return err
		}

		headCommits = append(headCommits, cm)
	}

	// DFS the commit graph find a unique new tag for all existing tags in every table in history
	globalMapping := make(map[string]map[uint64]uint64)
	globalCtr := new(uint64)

	replay := func(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue) (*doltdb.RootValue, error) {
		err := buildGlobalTagMapping(ctx, root, globalMapping, globalCtr)

		if err != nil {
			return nil, err
		}

		return root, nil
	}

	_, err := rebase(ctx, ddb, replay, entireHistory, headCommits...)

	if err != nil {
		return err
	}

	if len(branches) != len(headCommits) {
		panic("error in uniquifying tags")
	}

	for idx, dRef := range branches {
		var err error
		newCommit := headCommits[idx]
		for tblName, tableMapping := range globalMapping {
			// missing tables will be ignored
			newCommit, err = TagRebaseForCommit(ctx, newCommit, ddb, tblName, tableMapping)

			if err != nil {
				return err
			}
		}
		err = ddb.DeleteBranch(ctx, dRef)

		if err != nil {
			return err
		}

		err = ddb.NewBranchAtCommit(ctx, dRef, newCommit)

		if err != nil {
			return err
		}
	}

	return nil
}

func buildGlobalTagMapping(ctx context.Context, root *doltdb.RootValue, globalMapping map[string]map[uint64]uint64, globalCtr *uint64) error  {
	tblNames, err := root.GetTableNames(ctx)

	if err != nil {
		return err
	}

	for _, tn := range tblNames {
		if doltdb.IsSystemTable(tn) {
			continue
		}

		if _, found := globalMapping[tn]; !found {
			globalMapping[tn] = make(map[uint64]uint64)
		}

		t, _, err := root.GetTable(ctx, tn)

		if err != nil {
			return err
		}

		sch, err := t.GetSchema(ctx)

		if err != nil {
			return err
		}


		for _, t := range sch.GetAllCols().Tags {
			if _, found := globalMapping[tn][t]; !found {
				globalMapping[tn][t] = *globalCtr
				println(*globalCtr)
				atomic.AddUint64(globalCtr, 1)
			}
		}
	}
	return nil
}