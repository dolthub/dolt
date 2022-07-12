// Copyright 2022 Dolthub, Inc.
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
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type MergeOpts struct {
	IsCherryPick bool
}

type RootMerger struct {
	left  *doltdb.RootValue
	right *doltdb.RootValue
	anc   *doltdb.RootValue

	rightHash hash.Hash
	ancHash   hash.Hash

	vrw types.ValueReadWriter
	ns  tree.NodeStore
}

// NewMerger creates a new merger utility object.
func NewMerger(left, right, anc *doltdb.RootValue, vrw types.ValueReadWriter, ns tree.NodeStore) (*RootMerger, error) {
	rightHash, err := right.HashOf()
	if err != nil {
		return nil, err
	}
	ancHash, err := anc.HashOf()
	if err != nil {
		return nil, err
	}

	return &RootMerger{
		left:      left,
		right:     right,
		anc:       anc,
		rightHash: rightHash,
		ancHash:   ancHash,
		vrw:       vrw,
		ns:        ns,
	}, nil
}

// MergeTable merges schema and table data for the table tblName.
func (merger *RootMerger) MergeTable(ctx context.Context, tblName string, opts editor.Options, mergeOpts MergeOpts) (*doltdb.Table, *MergeStats, error) {
	rootHasTable, tbl, rootSchema, rootHash, err := getTableInfoFromRoot(ctx, tblName, merger.left)
	if err != nil {
		return nil, nil, err
	}

	mergeHasTable, mergeTbl, mergeSchema, mergeHash, err := getTableInfoFromRoot(ctx, tblName, merger.right)
	if err != nil {
		return nil, nil, err
	}

	ancHasTable, ancTbl, ancSchema, ancHash, err := getTableInfoFromRoot(ctx, tblName, merger.anc)
	if err != nil {
		return nil, nil, err
	}

	var ancRows durable.Index
	// only used by new storage format
	var ancIndexSet durable.IndexSet
	if ancHasTable {
		ancRows, err = ancTbl.GetRowData(ctx)
		if err != nil {
			return nil, nil, err
		}
		ancIndexSet, err = ancTbl.GetIndexSet(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	{ // short-circuit logic

		// Nothing changed
		if rootHasTable && mergeHasTable && ancHasTable && rootHash == mergeHash && rootHash == ancHash {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		}

		// Both made identical changes
		// For keyless tables, this counts as a conflict
		if rootHasTable && mergeHasTable && rootHash == mergeHash && !schema.IsKeyless(rootSchema) {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		}

		// One or both added this table
		if !ancHasTable {
			if mergeHasTable && rootHasTable {
				if schema.SchemasAreEqual(rootSchema, mergeSchema) {
					// If both added the same table, pretend it was in the ancestor all along with no data
					// Don't touch ancHash to avoid triggering other short-circuit logic below
					ancHasTable, ancSchema, ancTbl = true, rootSchema, tbl
					ancRows, err = durable.NewEmptyIndex(ctx, merger.vrw, merger.ns, ancSchema)
					if err != nil {
						return nil, nil, err
					}
					ancIndexSet, err = durable.NewIndexSetWithEmptyIndexes(ctx, merger.vrw, merger.ns, ancSchema)
					if err != nil {
						return nil, nil, err
					}
				} else {
					return nil, nil, ErrSameTblAddedTwice
				}
			} else if rootHasTable {
				// fast-forward
				return tbl, &MergeStats{Operation: TableUnmodified}, nil
			} else {
				// fast-forward
				return mergeTbl, &MergeStats{Operation: TableAdded}, nil
			}
		}

		// Deleted in both, fast-forward
		if ancHasTable && !rootHasTable && !mergeHasTable {
			return nil, &MergeStats{Operation: TableRemoved}, nil
		}

		// Deleted in root or in merge, either a conflict (if any changes in other root) or else a fast-forward
		if ancHasTable && (!rootHasTable || !mergeHasTable) {
			if mergeOpts.IsCherryPick && rootHasTable && !mergeHasTable {
				// TODO : this is either drop table or rename table case
				// We can delete only if the table in current HEAD and parent commit contents are exact the same (same schema and same data);
				// otherwise, return ErrTableDeletedAndModified
				// We need to track renaming of a table --> the renamed table could be added as new table
				return nil, &MergeStats{Operation: TableModified}, errors.New(fmt.Sprintf("schema changes not supported: %s table was renamed or dropped in cherry-pick commit", tblName))
			}

			if (mergeHasTable && mergeHash != ancHash) ||
				(rootHasTable && rootHash != ancHash) {
				return nil, nil, ErrTableDeletedAndModified
			}
			// fast-forward
			return nil, &MergeStats{Operation: TableRemoved}, nil
		}

		// Changes only in root, table unmodified
		if mergeHash == ancHash {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		}

		// Changes only in merge root, fast-forward
		// TODO : no fast-forward when cherry-picking for now
		if !mergeOpts.IsCherryPick && rootHash == ancHash {
			ms := MergeStats{Operation: TableModified}
			if rootHash != mergeHash {
				ms, err = calcTableMergeStats(ctx, tbl, mergeTbl)
				if err != nil {
					return nil, nil, err
				}
			}
			return mergeTbl, &ms, nil
		}
	}

	if mergeOpts.IsCherryPick && !schema.SchemasAreEqual(rootSchema, mergeSchema) {
		return nil, nil, errors.New(fmt.Sprintf("schema changes not supported: %s table schema does not match in current HEAD and cherry-pick commit.", tblName))
	}

	postMergeSchema, schConflicts, err := SchemaMerge(rootSchema, mergeSchema, ancSchema, tblName)
	if err != nil {
		return nil, nil, err
	}
	if schConflicts.Count() != 0 {
		// error on schema conflicts for now
		return nil, nil, schConflicts.AsError()
	}

	updatedTbl, err := tbl.UpdateSchema(ctx, postMergeSchema)
	if err != nil {
		return nil, nil, err
	}

	if types.IsFormat_DOLT_1(updatedTbl.Format()) {
		updatedTbl, err = mergeTableArtifacts(ctx, tblName, tbl, mergeTbl, ancTbl, updatedTbl)
		if err != nil {
			return nil, nil, err
		}

		var stats *MergeStats
		updatedTbl, stats, err = mergeTableData(
			ctx,
			merger.vrw,
			merger.ns,
			tblName,
			postMergeSchema, rootSchema, mergeSchema, ancSchema,
			tbl, mergeTbl, updatedTbl,
			ancRows,
			ancIndexSet,
			merger.rightHash,
			merger.ancHash)
		if err != nil {
			return nil, nil, err
		}

		n, err := updatedTbl.NumRowsInConflict(ctx)
		if err != nil {
			return nil, nil, err
		}
		stats.Conflicts = int(n)

		updatedTbl, err = mergeAutoIncrementValues(ctx, tbl, mergeTbl, updatedTbl)
		if err != nil {
			return nil, nil, err
		}
		return updatedTbl, stats, nil
	}

	// If any indexes were added during the merge, then we need to generate their row data to add to our updated table.
	addedIndexesSet := make(map[string]string)
	for _, index := range postMergeSchema.Indexes().AllIndexes() {
		addedIndexesSet[strings.ToLower(index.Name())] = index.Name()
	}
	for _, index := range rootSchema.Indexes().AllIndexes() {
		delete(addedIndexesSet, strings.ToLower(index.Name()))
	}
	for _, addedIndex := range addedIndexesSet {
		newIndexData, err := editor.RebuildIndex(ctx, updatedTbl, addedIndex, opts)
		if err != nil {
			return nil, nil, err
		}
		updatedTbl, err = updatedTbl.SetNomsIndexRows(ctx, addedIndex, newIndexData)
		if err != nil {
			return nil, nil, err
		}
	}

	updatedTblEditor, err := editor.NewTableEditor(ctx, updatedTbl, postMergeSchema, tblName, opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeRows, err := mergeTbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	resultTbl, cons, stats, err := mergeNomsTableData(ctx, merger.vrw, tblName, postMergeSchema, rows, mergeRows, durable.NomsMapFromIndex(ancRows), updatedTblEditor)
	if err != nil {
		return nil, nil, err
	}

	if cons.Len() > 0 {
		resultTbl, err = setConflicts(ctx, durable.ConflictIndexFromNomsMap(cons, merger.vrw), tbl, mergeTbl, ancTbl, resultTbl)
		if err != nil {
			return nil, nil, err
		}
		stats.Conflicts = int(cons.Len())
	}

	resultTbl, err = mergeAutoIncrementValues(ctx, tbl, mergeTbl, resultTbl)
	if err != nil {
		return nil, nil, err
	}

	return resultTbl, stats, nil
}

func setConflicts(ctx context.Context, cons durable.ConflictIndex, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table) (*doltdb.Table, error) {
	ancSch, err := ancTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	cs := conflict.NewConflictSchema(ancSch, sch, mergeSch)

	tableToUpdate, err = tableToUpdate.SetConflicts(ctx, cs, cons)
	if err != nil {
		return nil, err
	}

	return tableToUpdate, nil
}

func getTableInfoFromRoot(ctx context.Context, tblName string, root *doltdb.RootValue) (
	ok bool,
	table *doltdb.Table,
	sch schema.Schema,
	h hash.Hash,
	err error,
) {
	table, ok, err = root.GetTable(ctx, tblName)
	if err != nil {
		return false, nil, nil, hash.Hash{}, err
	}

	if ok {
		h, err = table.HashOf()
		if err != nil {
			return false, nil, nil, hash.Hash{}, err
		}
		sch, err = table.GetSchema(ctx)
		if err != nil {
			return false, nil, nil, hash.Hash{}, err
		}
	}

	return ok, table, sch, h, nil
}

func calcTableMergeStats(ctx context.Context, tbl *doltdb.Table, mergeTbl *doltdb.Table) (MergeStats, error) {
	ms := MergeStats{Operation: TableModified}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeRows, err := mergeTbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	ae := atomicerr.New()
	ch := make(chan diff.DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := diff.Summary(ctx, ch, rows, mergeRows, sch, mergeSch)

		ae.SetIfError(err)
	}()

	for p := range ch {
		if ae.IsSet() {
			break
		}

		ms.Adds += int(p.Adds)
		ms.Deletes += int(p.Removes)
		ms.Modifications += int(p.Changes)
	}

	if err := ae.Get(); err != nil {
		return MergeStats{}, err
	}

	return ms, nil
}
