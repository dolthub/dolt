// Copyright 2020 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	ndiff "github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

const diffBufSize = 4096

// { tableName -> { oldTag -> newTag }}
type TagMapping map[string]map[uint64]uint64

// NeedsUniqueTagMigration checks if a repo needs a unique tags migration
func NeedsUniqueTagMigration(ctx context.Context, ddb *doltdb.DoltDB) (bool, error) {
	bb, err := ddb.GetBranches(ctx)

	if err != nil {
		return false, err
	}

	for _, b := range bb {
		cs, err := doltdb.NewCommitSpec(b.String())

		if err != nil {
			return false, err
		}

		c, err := ddb.Resolve(ctx, cs, nil)

		if err != nil {
			return false, err
		}

		// check if this head commit is an init commit
		n, err := c.NumParents()
		if err != nil {
			return false, err
		}
		if n == 0 {
			// init commits don't need migration
			continue
		}

		r, err := c.GetRootValue()

		if err != nil {
			return false, err
		}

		needToMigrate, err := doltdb.RootNeedsUniqueTagsMigration(r)
		if err != nil {
			return false, err
		}
		if needToMigrate {
			return true, nil
		}
	}

	return false, nil
}

// MigrateUniqueTags rebases the history of the repo to uniquify tags within branch histories.
func MigrateUniqueTags(ctx context.Context, dEnv *env.DoltEnv) error {
	builtTagMappings := make(map[hash.Hash]TagMapping)

	// DFS the commit graph find a unique new tag for all existing tags in every table in history
	replay := func(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue) (rebaseRoot *doltdb.RootValue, err error) {
		h, err := rebasedParentRoot.HashOf()
		if err != nil {
			return nil, err
		}

		parentTagMapping, found := builtTagMappings[h]
		if !found {
			parentTagMapping = make(TagMapping)
		}

		tagMapping, err := buildTagMapping(ctx, root, rebasedParentRoot, parentTagMapping)
		if err != nil {
			return nil, err
		}

		rebasedRoot, err := replayCommitWithNewTag(ctx, root, parentRoot, rebasedParentRoot, tagMapping)
		if err != nil {
			return nil, err
		}

		rh, err := rebasedRoot.HashOf()
		if err != nil {
			return nil, err
		}
		builtTagMappings[rh] = tagMapping

		return rebasedRoot, nil
	}

	return AllBranchesByRoots(ctx, dEnv, replay, EntireHistory())
}

// TagRebaseForRef rebases the provided DoltRef, swapping all tags in the TagMapping.
func TagRebaseForRef(ctx context.Context, dRef ref.DoltRef, ddb *doltdb.DoltDB, tagMapping TagMapping) (*doltdb.Commit, error) {
	cs, err := doltdb.NewCommitSpec(dRef.String())

	if err != nil {
		return nil, err
	}

	cm, err := ddb.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, err
	}

	rebasedCommits, err := TagRebaseForCommits(ctx, ddb, tagMapping, cm)

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
		n, err := cm.NumParents()
		if err != nil {
			return false, err
		}
		exists, err := tagExistsInHistory(ctx, cm, tm)
		if err != nil {
			return false, err
		}
		return (n > 0) && exists, nil
	}

	rcs, err := rebase(ctx, ddb, wrapReplayRootFn(replay), nerf, startingCommits...)

	if err != nil {
		return nil, err
	}

	return rcs, nil
}

func replayCommitWithNewTag(ctx context.Context, root, parentRoot, rebasedParentRoot *doltdb.RootValue, tm TagMapping) (*doltdb.RootValue, error) {

	tableNames, err := doltdb.UnionTableNames(ctx, root, rebasedParentRoot)

	if err != nil {
		return nil, err
	}

	newRoot := rebasedParentRoot
	for _, tblName := range tableNames {

		tbl, found, err := root.GetTable(ctx, tblName)
		if err != nil {
			return nil, err
		}
		if !found {
			// table was deleted since parent commit
			ok, err := newRoot.HasTable(ctx, tblName)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("error rebasing, table %s not found in rebasedParentRoot", tblName)
			}

			newRoot, err = newRoot.RemoveTables(ctx, false, tblName)

			if err != nil {
				return nil, err
			}

			continue
		}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		// only rebase this table if we have a mapping for it, and at least one of the
		// tags in the mapping is present in its schema at this commit
		tableNeedsRebasing := false
		tableMapping, found := tm[tblName]
		if found {
			_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
				if _, found = tableMapping[tag]; found {
					tableNeedsRebasing = true
				}
				return tableNeedsRebasing, nil
			})
		}

		if !tableNeedsRebasing {
			newRoot, err = newRoot.PutTable(ctx, tblName, tbl)
			if err != nil {
				return nil, err
			}
		}

		parentTblName := tblName

		// schema rebase
		schCC := schema.NewColCollection()
		err = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if newTag, found := tableMapping[tag]; found {
				col.Tag = newTag
			}
			schCC = schCC.Append(col)
			return false, nil
		})

		if err != nil {
			return nil, err
		}

		rebasedSch, err := schema.SchemaFromCols(schCC)
		if err != nil {
			return nil, err
		}

		for _, index := range sch.Indexes().AllIndexes() {
			_, err = rebasedSch.Indexes().AddIndexByColNames(
				index.Name(),
				index.ColumnNames(),
				schema.IndexProperties{
					IsUnique:      index.IsUnique(),
					IsUserDefined: index.IsUserDefined(),
					Comment:       index.Comment(),
				},
			)
			if err != nil {
				return nil, err
			}
		}

		// super schema rebase
		ss, _, err := root.GetSuperSchema(ctx, tblName)

		if err != nil {
			return nil, err
		}

		rebasedSS, err := ss.RebaseTag(tableMapping)
		if err != nil {
			return nil, err
		}

		// row rebase
		var parentRows types.Map
		parentTbl, found, err := parentRoot.GetTable(ctx, tblName)
		if err != nil {
			return nil, err
		}

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
		var rebasedParentSch schema.Schema
		rebasedParentTbl, found, err := rebasedParentRoot.GetTable(ctx, parentTblName)
		if err != nil {
			return nil, err
		}
		if found && rebasedParentTbl != nil {
			rebasedParentRows, err = rebasedParentTbl.GetRowData(ctx)
			if err != nil {
				return nil, err
			}
			rebasedParentSch, err = rebasedParentTbl.GetSchema(ctx)
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

		rebasedParentRows, err = dropValsForDeletedColumns(ctx, root.VRW().Format(), rebasedParentRows, rebasedSch, rebasedParentSch)

		if err != nil {
			return nil, err
		}

		rebasedRows, err := replayRowDiffs(ctx, rebasedParentRoot.VRW(), rebasedSch, rows, parentRows, rebasedParentRows, tableMapping)

		if err != nil {
			return nil, err
		}

		emptyMap, err := types.NewMap(ctx, root.VRW()) // migration predates secondary indexes
		if err != nil {
			return nil, err
		}

		// migration predates AUTO_INCREMENT support
		// so we don't need to copy the value here
		var autoVal types.Value = nil

		rebasedTable, err := doltdb.NewTable(ctx, rebasedParentRoot.VRW(), rebasedSch, rebasedRows, emptyMap, autoVal)

		if err != nil {
			return nil, err
		}

		newRoot, err = newRoot.PutSuperSchema(ctx, tblName, rebasedSS)

		if err != nil {
			return nil, err
		}

		// create new RootValue by overwriting table with rebased rows and schema
		newRoot, err = newRoot.PutTable(ctx, tblName, rebasedTable)

		if err != nil {
			return nil, err
		}
	}
	return newRoot, nil
}

func replayRowDiffs(ctx context.Context, vrw types.ValueReadWriter, rSch schema.Schema, rows, parentRows, rebasedParentRows types.Map, tagMapping map[uint64]uint64) (res types.Map, err error) {
	unmappedTags := set.NewUint64Set(rSch.GetAllCols().Tags)
	tm := make(map[uint64]uint64)
	for ot, nt := range tagMapping {
		tm[ot] = nt
		unmappedTags.Remove(nt)
	}
	for _, t := range unmappedTags.AsSlice() {
		tm[t] = t
	}

	nmu := noms.NewNomsMapUpdater(ctx, vrw, rebasedParentRows, rSch, func(stats types.AppliedEditStats) {})

	ad := diff.NewRowDiffer(ctx, rSch, rSch, diffBufSize)
	// get all differences (including merges) between original commit and its parent
	ad.Start(ctx, parentRows, rows)
	defer func() {
		if cerr := ad.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	hasMore := true
	var diffs []*ndiff.Difference
	for hasMore {
		diffs, hasMore, err = ad.GetDiffs(diffBufSize/2, time.Second)
		if err != nil {
			return types.EmptyMap, err
		}

		for _, d := range diffs {
			if d.KeyValue == nil {
				panic("Unexpected commit diff result: with nil key value encountered")
			}

			key, newVal, err := modifyDifferenceTag(d, rows.Format(), rSch, tm)
			if err != nil {
				return types.EmptyMap, err
			}

			switch d.ChangeType {
			case types.DiffChangeAdded:
				err = nmu.WriteEdit(ctx, key, newVal)
			case types.DiffChangeRemoved:
				err = nmu.WriteEdit(ctx, key, nil)
			case types.DiffChangeModified:
				err = nmu.WriteEdit(ctx, key, newVal)
			}

			if err != nil {
				return types.EmptyMap, err
			}
		}
	}

	err = nmu.Close(ctx)
	if err != nil {
		return types.EmptyMap, err
	}

	return nmu.GetMap(), nil
}

func dropValsForDeletedColumns(ctx context.Context, nbf *types.NomsBinFormat, rows types.Map, sch, parentSch schema.Schema) (types.Map, error) {
	if parentSch == nil {
		return rows, nil
	}

	deletedCols := schema.ColCollectionSetDifference(parentSch.GetAllCols(), sch.GetAllCols())

	if deletedCols.Size() == 0 {
		return rows, nil
	}

	re := rows.Edit()

	mi, err := rows.BufferedIterator(ctx)

	if err != nil {
		return types.EmptyMap, err
	}

	for {
		k, v, err := mi.Next(ctx)

		if k == nil || v == nil {
			break
		}
		if err != nil {
			return types.EmptyMap, err
		}

		ktv, err := row.ParseTaggedValues(k.(types.Tuple))

		if err != nil {
			return types.EmptyMap, err
		}

		remove := false
		for keytag := range ktv {
			// if we've changed the PK, remove this row
			if _, found := sch.GetPKCols().GetByTag(keytag); !found {
				remove = true
				break
			}
		}
		if remove {
			re.Remove(k)
			continue
		}

		vtv, err := row.ParseTaggedValues(v.(types.Tuple))

		if err != nil {
			return types.EmptyMap, err
		}

		for valtag := range vtv {
			if _, found := sch.GetNonPKCols().GetByTag(valtag); !found {
				delete(vtv, valtag)
			}
		}

		re.Set(k, vtv.NomsTupleForNonPKCols(nbf, sch.GetNonPKCols()))
	}

	prunedRowData, err := re.Map(ctx)

	if err != nil {
		return types.EmptyMap, nil
	}

	return prunedRowData, nil
}

func modifyDifferenceTag(d *ndiff.Difference, nbf *types.NomsBinFormat, rSch schema.Schema, tagMapping map[uint64]uint64) (keyTup types.LesserValuable, valTup types.Valuable, err error) {

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

	keyTup = newKtv.NomsTupleForPKCols(nbf, rSch.GetPKCols())

	valTup = d.NewValue
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
		valTup = newTv.NomsTupleForNonPKCols(nbf, rSch.GetNonPKCols())
	}

	return keyTup, valTup, nil
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
			if _, found := ss.GetByTag(oldTag); found {
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

func buildTagMapping(ctx context.Context, root, rebasedParentRoot *doltdb.RootValue, parentTagMapping TagMapping) (TagMapping, error) {
	tagMapping := parentTagMapping

	// create mappings for new columns
	tblNames, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	rss, err := doltdb.GetRootValueSuperSchema(ctx, rebasedParentRoot)

	if err != nil {
		return nil, err
	}

	existingRebasedTags := set.NewUint64Set(rss.AllTags())

	for _, tn := range tblNames {
		if doltdb.HasDoltPrefix(tn) {
			err = handleSystemTableMappings(ctx, tn, root, tagMapping)
			if err != nil {
				return nil, err
			}
			continue
		}

		if _, found := tagMapping[tn]; !found {
			tagMapping[tn] = make(map[uint64]uint64)
		}

		t, _, err := root.GetTable(ctx, tn)
		if err != nil {
			return nil, err
		}

		sch, err := t.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		var newColNames []string
		var newColKinds []types.NomsKind
		var oldTags []uint64
		var existingColKinds []types.NomsKind
		_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			_, found := tagMapping[tn][tag]
			if !found {
				newColNames = append(newColNames, col.Name)
				newColKinds = append(newColKinds, col.Kind)
				oldTags = append(oldTags, tag)
			} else {
				existingColKinds = append(existingColKinds, col.Kind)
			}
			return false, nil
		})

		// generate tags with the same method as root.GenerateTagsForNewColumns()
		newTags := make([]uint64, len(newColNames))
		for i := range newTags {
			newTags[i] = schema.AutoGenerateTag(existingRebasedTags, tn, existingColKinds, newColNames[i], newColKinds[i])
			existingColKinds = append(existingColKinds, newColKinds[i])
			existingRebasedTags.Add(newTags[i])
		}

		for i, ot := range oldTags {
			tagMapping[tn][ot] = newTags[i]
		}
	}

	err = validateTagMapping(tagMapping)

	if err != nil {
		return nil, err
	}

	return tagMapping, nil
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
			doltdb.DocPkColumnName:   schema.DocNameTag,
			doltdb.DocTextColumnName: schema.DocTextTag,
		}
	case doltdb.DoltQueryCatalogTableName:
		newTagsByColName = map[string]uint64{
			doltdb.QueryCatalogIdCol:          schema.QueryCatalogIdTag,
			doltdb.QueryCatalogOrderCol:       schema.QueryCatalogOrderTag,
			doltdb.QueryCatalogNameCol:        schema.QueryCatalogNameTag,
			doltdb.QueryCatalogQueryCol:       schema.QueryCatalogQueryTag,
			doltdb.QueryCatalogDescriptionCol: schema.QueryCatalogDescriptionTag,
		}
	case doltdb.SchemasTableName:
		newTagsByColName = map[string]uint64{
			doltdb.SchemasTablesIdCol:       schema.DoltSchemasIdTag,
			doltdb.SchemasTablesTypeCol:     schema.DoltSchemasTypeTag,
			doltdb.SchemasTablesNameCol:     schema.DoltSchemasNameTag,
			doltdb.SchemasTablesFragmentCol: schema.DoltSchemasFragmentTag,
		}
	}

	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		globalMapping[tblName][tag] = newTagsByColName[col.Name]
		return false, nil
	})

	return nil
}
