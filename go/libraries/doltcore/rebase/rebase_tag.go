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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	ndiff "github.com/liquidata-inc/dolt/go/store/diff"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// { tableName -> { oldTag -> newTag }}
type TagMapping map[string]map[uint64]uint64

// MaybeMigrateUniqueTags checks if a repo was created before the unique tags constraint and migrates it if necessary.
func MaybeMigrateUniqueTags(ctx context.Context, dEnv *env.DoltEnv) error {
	bb, err := dEnv.DoltDB.GetBranches(ctx)

	if err != nil {
		return err
	}

	migrate := false
	for _, b := range bb {
		cs, err := doltdb.NewCommitSpec("head", b.String())

		if err != nil {
			return err
		}

		c, err := dEnv.DoltDB.Resolve(ctx, cs)

		if err != nil {
			return err
		}

		r, err := c.GetRootValue()

		if err != nil {
			return err
		}

		needToMigrate, err := doltdb.RootNeedsUniqueTagsMigration(r)
		if err != nil {
			return err
		}
		if needToMigrate {
			migrate = true
		}
	}

	if !migrate {
		return nil
	}

	cwbSpec := dEnv.RepoState.CWBHeadSpec()
	dd, err := dEnv.GetAllValidDocDetails()

	if err != nil {
		return err
	}

	err = MigrateUniqueTags(ctx, dEnv.DoltDB, bb)

	if err != nil {
		return err
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cwbSpec)

	if err != nil {
		return err
	}

	r, err := cm.GetRootValue()

	if err != nil {
		return err
	}

	_, err = dEnv.UpdateStagedRoot(ctx, r)

	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingRoot(ctx, r)

	if err != nil {
		return err
	}

	err = dEnv.PutDocsToWorking(ctx, dd)

	if err != nil {
		return err
	}

	_, err = dEnv.PutDocsToStaged(ctx, dd)
	return err
}

// TagRebaseForRef rebases the provided DoltRef, swapping all tags in the TagMapping.
func TagRebaseForRef(ctx context.Context, dRef ref.DoltRef, ddb *doltdb.DoltDB, tagMapping TagMapping) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec("head", dRef.String())

	if err != nil {
		return nil, err
	}

	cm, err := ddb.Resolve(ctx, cs)

	if err != nil {
		return nil, err
	}

	rebasedCommits, err := TagRebaseForCommits(ctx, ddb, tagMapping, cm)

	if err != nil {
		return nil, err
	}

	err = ddb.DeleteBranch(ctx, dRef)

	if err != nil {
		return nil, err
	}

	err = ddb.NewBranchAtCommit(ctx, dRef, rebasedCommits[0])

	if err != nil {
		return nil, err
	}

	return rebasedCommits[0], nil
}

// TagRebaseForReg rebases the provided Commits, swapping all tags in the TagMapping.
func TagRebaseForCommits(ctx context.Context, ddb *doltdb.DoltDB, tm TagMapping, startingCommits ...*doltdb.Commit) ([]*doltdb.Commit, error) {
	err := validateTagMapping(tm)

	if err != nil {
		return nil, err
	}

	replay := func(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue) (rebaseRoot *doltdb.RootValue, err error) {
		return replayCommitWithNewTag(ctx, root, parentRoot, rebasedParentRoot, tm)
	}

	nerf := func(ctx context.Context, cm *doltdb.Commit) (b bool, err error) {
		return tagExistsInHistory(ctx, cm, tm)
	}

	rcs, err := rebase(ctx, ddb, replay, nerf, startingCommits...)

	if err != nil {
		return nil, err
	}

	return rcs, nil
}

func replayCommitWithNewTag(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue, tm TagMapping) (*doltdb.RootValue, error) {

	newRoot := root
	for tblName, tableMapping := range tm {

		tbl, found, err := newRoot.GetTable(ctx, tblName)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		// tags may not exist in this commit
		tagExists := false
		for oldTag, _ := range tableMapping {
			if _, found := sch.GetAllCols().GetByTag(oldTag); found {
				tagExists = true
				break
			}
		}
		if !tagExists {
			continue
		}

		parentTblName := tblName

		// schema rebase
		schCC, _ := schema.NewColCollection()
		err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if newTag, found := tableMapping[tag]; found {
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
		ss, _, err := newRoot.GetSuperSchema(ctx, tblName)

		if err != nil {
			return nil, err
		}

		rebasedSS, err := ss.RebaseTag(tableMapping)

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

		rebasedRows, err := replayRowDiffs(ctx, rebasedSch, rows, parentRows, rebasedParentRows, tableMapping)

		if err != nil {
			return nil, err
		}

		rebasedSchVal, err := encoding.MarshalSchemaAsNomsValue(ctx, rebasedParentRoot.VRW(), rebasedSch)

		if err != nil {
			return nil, err
		}

		rsh, _ := rebasedSchVal.Hash(newRoot.VRW().Format())
		rshs := rsh.String()
		fmt.Println(rshs)

		rebasedTable, err := doltdb.NewTable(ctx, rebasedParentRoot.VRW(), rebasedSchVal, rebasedRows)

		if err != nil {
			return nil, err
		}

		rebasedRoot, err := newRoot.PutSuperSchema(ctx, tblName, rebasedSS)

		if err != nil {
			return nil, err
		}

		// create new RootValue by overwriting table with rebased rows and schema
		newRoot, err = doltdb.PutTable(ctx, rebasedRoot, rebasedRoot.VRW(), tblName, rebasedTable)

		if err != nil {
			return nil, err
		}
	}
	return newRoot, nil
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

	newKtv := make(row.TaggedValues)
	for tag, val := range ktv {
		newTag, found := tagMapping[tag]
		if !found {
			newTag = tag
		}
		newKtv[newTag] = val
	}

	key = newKtv.NomsTupleForTags(nbf, rSch.GetPKCols().Tags, true)

	val = d.NewValue
	if d.NewValue != nil {
		tv, err := row.ParseTaggedValues(d.NewValue.(types.Tuple))

		if err != nil {
			return nil, nil, err
		}

		newTv := make(row.TaggedValues)
		for tag, val := range tv {
			newTag, found := tagMapping[tag]
			if !found {
				newTag = tag
			}
			newTv[newTag] = val
		}

		val = newTv.NomsTupleForTags(nbf, rSch.GetNonPKCols().Tags, false)
	}

	return key, val, nil
}

func tagExistsInHistory(ctx context.Context, c *doltdb.Commit, tagMapping TagMapping) (bool, error) {

	crt, err := c.GetRootValue()

	if err != nil {
		return false, err
	}

	tblNames, err := crt.GetTableNames(ctx)

	if err != nil {
		return false, err
	}

	for _, tn := range tblNames {
		tblMapping, found := tagMapping[tn]
		if !found {
			continue
		}

		ss, _, err := crt.GetSuperSchema(ctx, tn)

		if err != nil {
			return false, err
		}

		for oldTag, _ := range tblMapping {
			if _, found := ss.GetColumn(oldTag); found {
				return true, nil
			}
		}
	}

	return false, nil
}

func validateTagMapping(tagMapping TagMapping) error {
	for tblName, tblMapping := range tagMapping {
		newTags := make(map[uint64]struct{})
		for _, nt := range tblMapping {
			if _, found := newTags[nt]; found {
				return fmt.Errorf("duplicate tag %d found in tag mapping for table %s", nt, tblName)
			}
			newTags[nt] = struct{}{}
		}
	}
	return nil
}

func MigrateUniqueTags(ctx context.Context, ddb *doltdb.DoltDB, branches []ref.DoltRef) error {
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

	newCommits, err := TagRebaseForCommits(ctx, ddb, globalMapping, headCommits...)

	if err != nil {
		return err
	}

	for idx, dRef := range branches {

		err = ddb.DeleteBranch(ctx, dRef)

		if err != nil {
			return err
		}

		err = ddb.NewBranchAtCommit(ctx, dRef, newCommits[idx])

		if err != nil {
			return err
		}
	}

	return nil
}

func buildGlobalTagMapping(ctx context.Context, root *doltdb.RootValue, globalMapping map[string]map[uint64]uint64, globalCtr *uint64) error {
	tblNames, err := root.GetTableNames(ctx)

	if err != nil {
		return err
	}

	for _, tn := range tblNames {
		if doltdb.HasDoltPrefix(tn) {
			err = handleSystemTableMappings(ctx, tn, root, globalMapping)
			if err != nil {
				return err
			}
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
				atomic.AddUint64(globalCtr, 1)
			}
		}
	}
	return nil
}

func handleSystemTableMappings(ctx context.Context, tblName string, root *doltdb.RootValue, globalMapping map[string]map[uint64]uint64) error {
	globalMapping[tblName] = make(map[uint64]uint64)

	t, _, err := root.GetTable(ctx, tblName)

	if err != nil {
		return err
	}

	sch, err := t.GetSchema(ctx)

	if err != nil {
		return err
	}

	var newTagsByColName map[string]uint64
	switch tblName {
	case doltdb.DocTableName:
		newTagsByColName = map[string]uint64{
			doltdb.DocPkColumnName:   doltdb.DocNameTag,
			doltdb.DocTextColumnName: doltdb.DocTextTag,
		}
	case doltdb.DoltQueryCatalogTableName:
		newTagsByColName = map[string]uint64{
			//sqle.QueryCatalogIdCol:          doltdb.QueryCatalogIdTag,
			//sqle.QueryCatalogOrderCol:       doltdb.QueryCatalogOrderTag,
			//sqle.QueryCatalogNameCol:        doltdb.QueryCatalogNameTag,
			//sqle.QueryCatalogQueryCol:       doltdb.QueryCatalogQueryTag,
			//sqle.QueryCatalogDescriptionCol: doltdb.QueryCatalogDescriptionTag,
			"id":          doltdb.QueryCatalogIdTag,
			"display_order":       doltdb.QueryCatalogOrderTag,
			"name":        doltdb.QueryCatalogNameTag,
			"query":       doltdb.QueryCatalogQueryTag,
			"description": doltdb.QueryCatalogDescriptionTag,
		}
	case doltdb.SchemasTableName:
		newTagsByColName = map[string]uint64{
			//sqle.SchemasTablesTypeCol:     doltdb.DoltSchemasTypeTag,
			//sqle.SchemasTablesNameCol:     doltdb.DoltSchemasNameTag,
			//sqle.SchemasTablesFragmentCol: doltdb.DoltSchemasFragmentTag,
			"type":     doltdb.DoltSchemasTypeTag,
			"name":     doltdb.DoltSchemasNameTag,
			"fragment": doltdb.DoltSchemasFragmentTag,
		}
	}

	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		globalMapping[tblName][tag] = newTagsByColName[col.Name]
		return false, nil
	})

	return nil
}
