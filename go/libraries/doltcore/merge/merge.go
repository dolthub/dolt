// Copyright 2019 Dolthub, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	json2 "github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/libraries/utils/valutil"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrFastForward = errors.New("fast forward")
var ErrSameTblAddedTwice = errors.New("table with same name added in 2 commits can't be merged")
var ErrTableDeletedAndModified = errors.New("conflict: table with same name deleted and modified ")

type Merger struct {
	root      *doltdb.RootValue
	mergeRoot *doltdb.RootValue
	ancRoot   *doltdb.RootValue
	vrw       types.ValueReadWriter
}

// NewMerger creates a new merger utility object.
func NewMerger(ctx context.Context, root, mergeRoot, ancRoot *doltdb.RootValue, vrw types.ValueReadWriter) *Merger {
	return &Merger{root, mergeRoot, ancRoot, vrw}
}

type cellWiseMerge struct {
	leftDiff  tree.Diff
	rightDiff tree.Diff
	merged    tree.Diff
}

// MergeTable merges schema and table data for the table tblName.
func (merger *Merger) MergeTable(ctx context.Context, tblName string, opts editor.Options) (*doltdb.Table, *MergeStats, error) {
	rootHasTable, tbl, rootSchema, rootHash, err := getTableInfoFromRoot(ctx, tblName, merger.root)
	if err != nil {
		return nil, nil, err
	}

	mergeHasTable, mergeTbl, mergeSchema, mergeHash, err := getTableInfoFromRoot(ctx, tblName, merger.mergeRoot)
	if err != nil {
		return nil, nil, err
	}

	ancHasTable, ancTbl, ancSchema, ancHash, err := getTableInfoFromRoot(ctx, tblName, merger.ancRoot)
	if err != nil {
		return nil, nil, err
	}

	var ancRows durable.Index
	if ancHasTable {
		ancRows, err = ancTbl.GetRowData(ctx)
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
					ancRows, _ = durable.NewEmptyIndex(ctx, merger.vrw, ancSchema)
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
		if rootHash == ancHash {
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

	if updatedTbl.Format() == types.Format_DOLT_1 {
		var stats *MergeStats
		updatedTbl, stats, err = mergeTableData(ctx, merger.vrw, postMergeSchema, rootSchema, mergeSchema, ancSchema, tbl, mergeTbl, ancTbl, updatedTbl)
		if err != nil {
			return nil, nil, err
		}

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
		ancSch, err := ancTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}

		mergeSch, err := mergeTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}

		cs := conflict.NewConflictSchema(ancSch, sch, mergeSch)

		resultTbl, err = resultTbl.SetConflicts(ctx, cs, cons)
		if err != nil {
			return nil, nil, err
		}
	}

	resultTbl, err = mergeAutoIncrementValues(ctx, tbl, mergeTbl, resultTbl)
	if err != nil {
		return nil, nil, err
	}

	return resultTbl, stats, nil
}

// mergeTableData three-way merges rows and indexes for a given table. First,
// the primary row data is merged, then secondary indexes are merged. In the
// process of merging the primary row data, we may need to perform cell-wise
// merges. Since a cell-wise merge result neither contains the values from the
// root branch or the merge branch we also need to update the secondary indexes
// prior to merging them.
//
// Each cell-wise merge reverts the corresponding index entries in the root
// branch, and modifies index entries in the merge branch. The merge branch's
// entries are set to values consistent the cell-wise merge result. When the
// root and merge secondary indexes are merged, they will produce entries
// consistent with the primary row data.
func mergeTableData(ctx context.Context, vrw types.ValueReadWriter, postMergeSchema, rootSchema, mergeSchema, ancSchema schema.Schema, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table) (*doltdb.Table, *MergeStats, error) {
	group, gCtx := errgroup.WithContext(ctx)

	cellWiseMerges := make(chan cellWiseMerge)
	var updatedTable *doltdb.Table
	var mergedData durable.Index

	group.Go(func() error {
		var err error
		// TODO (dhruv): update this function definition to return any conflicts
		updatedTable, mergedData, err = mergeProllyRowData(gCtx, postMergeSchema, rootSchema, mergeSchema, ancSchema, tbl, mergeTbl, ancTbl, tableToUpdate, cellWiseMerges)
		if err != nil {
			return err
		}
		close(cellWiseMerges)
		return nil
	})

	rootIndexSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, nil, err
	}
	mergeIndexSet, err := mergeTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, nil, err
	}

	group.Go(func() error {
		return updateProllySecondaryIndexes(gCtx, cellWiseMerges, rootSchema, mergeSchema, tbl, mergeTbl, &rootIndexSet, &mergeIndexSet)
	})

	err = group.Wait()
	if err != nil {
		return nil, nil, err
	}

	tbl, err = tbl.SetIndexSet(ctx, rootIndexSet)
	if err != nil {
		return nil, nil, err
	}
	mergeTbl, err = mergeTbl.SetIndexSet(ctx, mergeIndexSet)
	if err != nil {
		return nil, nil, err
	}

	updatedTable, err = mergeProllySecondaryIndexes(ctx, vrw, postMergeSchema, rootSchema, mergeSchema, ancSchema, mergedData, tbl, mergeTbl, ancTbl, updatedTable)
	if err != nil {
		return nil, nil, err
	}

	// TODO (dhruv): populate merge stats
	return updatedTable, &MergeStats{Operation: TableModified}, nil
}

// mergeProllyRowData merges the primary row table indexes of |tbl|, |mergeTbl|,
// and |ancTbl|. It stores the merged row data into |tableToUpdate| and returns the new value along with the row data.
func mergeProllyRowData(ctx context.Context, postMergeSchema, rootSch, mergeSch, ancSch schema.Schema, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table, cellWiseMerges chan cellWiseMerge) (*doltdb.Table, durable.Index, error) {
	rootR, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	mergeR, err := mergeTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	ancR, err := ancTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	rootRP := durable.ProllyMapFromIndex(rootR)
	mergeRP := durable.ProllyMapFromIndex(mergeR)
	ancRP := durable.ProllyMapFromIndex(ancR)

	vMerger := newValueMerger(postMergeSchema, rootSch, mergeSch, ancSch)

	conflicted := false
	mergedRP, err := prolly.MergeMaps(ctx, rootRP, mergeRP, ancRP, func(left, right tree.Diff) (tree.Diff, bool) {
		merged, isConflict := vMerger.tryMerge(val.Tuple(left.To), val.Tuple(right.To), val.Tuple(left.From))
		if isConflict {
			conflicted = true
			return tree.Diff{}, false
		}

		d := tree.Diff{
			Type: tree.ModifiedDiff,
			Key:  left.Key,
			From: left.From,
			To:   tree.Item(merged),
		}
		cellWiseMerges <- cellWiseMerge{left, right, d}

		return d, true
	})
	if err != nil {
		return nil, nil, err
	}
	if conflicted {
		return nil, nil, errors.New("row conflicts not supported yet")
	}

	updatedTbl, err := tableToUpdate.UpdateRows(ctx, durable.IndexFromProllyMap(mergedRP))
	if err != nil {
		return nil, nil, err
	}

	return updatedTbl, durable.IndexFromProllyMap(mergedRP), nil
}

var syncPool = pool.NewBuffPool()

type valueMerger struct {
	numCols                                int
	vD, lVD, rVD, bVD                      val.TupleDesc
	leftMapping, rightMapping, baseMapping map[int]int
}

func newValueMerger(merged, leftSch, rightSch, baseSch schema.Schema) *valueMerger {
	n := merged.GetNonPKCols().Size()
	leftMapping := make(map[int]int, n)
	rightMapping := make(map[int]int, n)
	baseMapping := make(map[int]int, n)

	for i, tag := range merged.GetNonPKCols().Tags {
		if j, ok := leftSch.GetNonPKCols().TagToIdx[tag]; ok {
			leftMapping[i] = j
		}
		if j, ok := rightSch.GetNonPKCols().TagToIdx[tag]; ok {
			rightMapping[i] = j
		}
		if j, ok := baseSch.GetNonPKCols().TagToIdx[tag]; ok {
			baseMapping[i] = j
		}
	}

	return &valueMerger{
		n,
		prolly.ValueDescriptorFromSchema(merged),
		prolly.ValueDescriptorFromSchema(leftSch),
		prolly.ValueDescriptorFromSchema(rightSch),
		prolly.ValueDescriptorFromSchema(baseSch),
		leftMapping,
		rightMapping,
		baseMapping,
	}
}

// tryMerge performs a cell-wise merge given left, right, and base cell value
// tuples. It returns the merged cell value tuple and a bool indicating if a
// conflict occurred. tryMerge should only be called if left and right produce
// non-identical diffs against base.
func (m *valueMerger) tryMerge(left, right, base val.Tuple) (val.Tuple, bool) {

	if base != nil && (left == nil) != (right == nil) {
		// One row deleted, the other modified
		return nil, true
	}

	// Because we have non-identical diffs, left and right are guaranteed to be
	// non-nil at this point.
	if left == nil || right == nil {
		panic("found nil left / right which should never occur")
	}

	processColumnFunc := func(i int) ([]byte, bool) {

		// missing columns are coerced into NULL column values
		var leftCol []byte
		if l, ok := m.leftMapping[i]; ok {
			leftCol = m.lVD.GetField(l, left)
		}
		var rightCol []byte
		if r, ok := m.rightMapping[i]; ok {
			rightCol = m.rVD.GetField(r, right)
		}

		if m.vD.Comparator().CompareValues(leftCol, rightCol, m.vD.Types[i]) == 0 {
			return leftCol, false
		}

		if base == nil {
			// Conflicting insert
			return nil, true
		}

		var baseVal []byte
		if b, ok := m.baseMapping[i]; ok {
			baseVal = m.bVD.GetField(b, base)
		}

		leftModified := m.vD.Comparator().CompareValues(leftCol, baseVal, m.vD.Types[i]) != 0
		rightModified := m.vD.Comparator().CompareValues(rightCol, baseVal, m.vD.Types[i]) != 0

		switch {
		case leftModified && rightModified:
			return nil, true
		case leftModified:
			return leftCol, false
		default:
			return rightCol, false
		}
	}

	mergedValues := make([][]byte, m.numCols)
	for i := 0; i < m.numCols; i++ {
		v, isConflict := processColumnFunc(i)
		if isConflict {
			return nil, true
		}
		mergedValues[i] = v
	}

	return val.NewTuple(syncPool, mergedValues...), false
}

// Given cellWiseMerge's sent on |cellWiseChan|, update the secondary indexes in
// |rootIndexSet| and |mergeIndexSet| such that when the index sets are merged,
// they produce entries consistent with the cell-wise merges.
func updateProllySecondaryIndexes(
	ctx context.Context,
	cellWiseChan chan cellWiseMerge,
	rootSchema, mergeSchema schema.Schema,
	tbl, mergeTbl *doltdb.Table,
	rootIndexSet, mergeIndexSet *durable.IndexSet) error {

	rootIdxs, err := getMutableSecondaryIdxs(ctx, rootSchema, tbl)
	if err != nil {
		return err
	}
	mergeIdxs, err := getMutableSecondaryIdxs(ctx, mergeSchema, mergeTbl)
	if err != nil {
		return err
	}

	select {
	case m, ok := <-cellWiseChan:
		if !ok {
			break
		}
		for _, idx := range rootIdxs {
			// Revert corresponding idx entry in left
			err = idx.UpdateEntry(ctx, val.Tuple(m.leftDiff.Key), val.Tuple(m.leftDiff.To), val.Tuple(m.leftDiff.From))
			if err != nil {
				return err
			}
		}
		for _, idx := range mergeIdxs {
			// Update corresponding idx entry to merged value in right
			err = idx.UpdateEntry(ctx, val.Tuple(m.rightDiff.Key), val.Tuple(m.rightDiff.To), val.Tuple(m.merged.To))
			if err != nil {
				return err
			}
		}
		// TODO (dhruv): periodically flush the MutableMaps
	case <-ctx.Done():
		return ctx.Err()
	}

	persistIndexMuts := func(indexSet *durable.IndexSet, idxs []writer.MutableSecondaryIdx) error {
		for _, idx := range idxs {
			m, err := idx.Map(ctx)
			if err != nil {
				return err
			}
			*indexSet, err = (*indexSet).PutIndex(ctx, idx.Name, durable.IndexFromProllyMap(m))
			if err != nil {
				return err
			}
		}

		return nil
	}

	err = persistIndexMuts(rootIndexSet, rootIdxs)
	if err != nil {
		return err
	}

	err = persistIndexMuts(mergeIndexSet, mergeIdxs)
	if err != nil {
		return err
	}

	return nil
}

// getMutableSecondaryIdxs returns a MutableSecondaryIdx for each secondary index
// defined in |schema| and |tbl|.
func getMutableSecondaryIdxs(ctx context.Context, schema schema.Schema, tbl *doltdb.Table) ([]writer.MutableSecondaryIdx, error) {
	indexSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	mods := make([]writer.MutableSecondaryIdx, schema.Indexes().Count())
	for i, index := range schema.Indexes().AllIndexes() {
		idx, err := indexSet.GetIndex(ctx, schema, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)

		mods[i] = writer.NewMutableSecondaryIdx(m, schema, index, syncPool)
	}

	return mods, nil
}

// mergeProllySecondaryIndexes merges the secondary indexes of the given |tbl|,
// |mergeTbl|, and |ancTbl|. It stores the merged indexes into |tableToUpdate|
// and returns its updated value.
func mergeProllySecondaryIndexes(ctx context.Context, vrw types.ValueReadWriter, postMergeSchema, rootSch, mergeSch, ancSch schema.Schema, mergedData durable.Index, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table) (*doltdb.Table, error) {
	rootSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	mergeSet, err := mergeTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	ancSet, err := ancTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	mergedSet, err := mergeProllyIndexSets(ctx, vrw, postMergeSchema, rootSch, mergeSch, ancSch, mergedData, rootSet, mergeSet, ancSet)
	if err != nil {
		return nil, err
	}
	updatedTbl, err := tableToUpdate.SetIndexSet(ctx, mergedSet)
	if err != nil {
		return nil, err
	}
	return updatedTbl, nil
}

// mergeProllyIndexSets merges the |root|, |merge|, and |anc| index sets based
// on the provided |postMergeSchema|. It returns the merged index set.
func mergeProllyIndexSets(ctx context.Context, vrw types.ValueReadWriter, postMergeSchema, rootSch, mergeSch, ancSch schema.Schema, mergedData durable.Index, root, merge, anc durable.IndexSet) (durable.IndexSet, error) {
	mergedIndexSet := durable.NewIndexSet(ctx, vrw)

	tryGetIdx := func(sch schema.Schema, iS durable.IndexSet, indexName string) (idx durable.Index, ok bool, err error) {
		ok = sch.Indexes().Contains(indexName)
		if ok {
			idx, err = iS.GetIndex(ctx, sch, indexName)
			if err != nil {
				return nil, false, err
			}
			return idx, true, nil
		}
		return nil, false, nil
	}

	// Based on the indexes in the post merge schema, merge the root, merge,
	// and ancestor indexes.
	for _, index := range postMergeSchema.Indexes().AllIndexes() {

		rootI, rootOK, err := tryGetIdx(rootSch, root, index.Name())
		if err != nil {
			return nil, err
		}
		mergeI, mergeOK, err := tryGetIdx(mergeSch, merge, index.Name())
		if err != nil {
			return nil, err
		}
		ancI, ancOK, err := tryGetIdx(ancSch, anc, index.Name())
		if err != nil {
			return nil, err
		}

		mergedIndex, err := func() (durable.Index, error) {
			if !rootOK || !mergeOK || !ancOK {
				mergedIndex, err := creation.BuildSecondaryProllyIndex(ctx, vrw, postMergeSchema, index, durable.ProllyMapFromIndex(mergedData))
				if err != nil {
					return nil, err
				}
				return mergedIndex, nil
			}

			left := durable.ProllyMapFromIndex(rootI)
			right := durable.ProllyMapFromIndex(mergeI)
			base := durable.ProllyMapFromIndex(ancI)

			var collision = false
			merged, err := prolly.MergeMaps(ctx, left, right, base, func(left, right tree.Diff) (tree.Diff, bool) {
				collision = true
				return tree.Diff{}, true
			})
			if err != nil {
				return nil, err
			}
			if collision {
				return nil, errors.New("collisions not implemented")
			}
			return durable.IndexFromProllyMap(merged), nil
		}()
		if err != nil {
			return nil, err
		}

		mergedIndexSet, err = mergedIndexSet.PutIndex(ctx, index.Name(), mergedIndex)
		if err != nil {
			return nil, err
		}
	}

	return mergedIndexSet, nil
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
	if tbl.Format() == types.Format_DOLT_1 {
		// TODO (dhruv): calculate stats for V1
		return ms, nil
	}

	rows, err := tbl.GetNomsRowData(ctx)

	if err != nil {
		return MergeStats{}, err
	}

	mergeRows, err := mergeTbl.GetNomsRowData(ctx)

	if err != nil {
		return MergeStats{}, err
	}

	ae := atomicerr.New()
	ch := make(chan diff.DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := diff.Summary(ctx, ch, rows, mergeRows)

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

type rowMergeResult struct {
	mergedRow    types.Value
	didCellMerge bool
	isConflict   bool
}

type rowMerger func(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (rowMergeResult, error)

type applicator func(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, rowData types.Map, stats *MergeStats, change types.ValueChanged) error

func mergeNomsTableData(ctx context.Context, vrw types.ValueReadWriter, tblName string, sch schema.Schema, rows, mergeRows, ancRows types.Map, tblEdit editor.TableEditor) (*doltdb.Table, types.Map, *MergeStats, error) {
	var rowMerge rowMerger
	var applyChange applicator
	if schema.IsKeyless(sch) {
		rowMerge = keylessRowMerge
		applyChange = applyKeylessChange
	} else {
		rowMerge = nomsPkRowMerge
		applyChange = applyNomsPkChange
	}

	changeChan, mergeChangeChan := make(chan types.ValueChanged, 32), make(chan types.ValueChanged, 32)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer close(changeChan)
		return rows.Diff(ctx, ancRows, changeChan)
	})
	eg.Go(func() error {
		defer close(mergeChangeChan)
		return mergeRows.Diff(ctx, ancRows, mergeChangeChan)
	})

	conflictValChan := make(chan types.Value)
	sm := types.NewStreamingMap(ctx, vrw, conflictValChan)
	stats := &MergeStats{Operation: TableModified}

	eg.Go(func() error {
		defer close(conflictValChan)

		var change, mergeChange types.ValueChanged
		for {
			// Get the next change from both a and b. If either diff(a, parent) or diff(b, parent) is
			// complete, aChange or bChange will get an empty types.ValueChanged containing a nil Value.
			// Generally, though, this allows us to proceed through both diffs in (key) order, considering
			// the "current" change from both diffs at the same time.
			if change.Key == nil {
				select {
				case change = <-changeChan:
					break
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if mergeChange.Key == nil {
				select {
				case mergeChange = <-mergeChangeChan:
					break
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			key, mergeKey := change.Key, mergeChange.Key

			// Both channels are producing zero values, so we're done.
			if key == nil && mergeKey == nil {
				break
			}

			var err error
			var processed bool
			if key != nil {
				mkNilOrKeyLess := mergeKey == nil
				if !mkNilOrKeyLess {
					mkNilOrKeyLess, err = key.Less(vrw.Format(), mergeKey)
					if err != nil {
						return err
					}
				}

				if mkNilOrKeyLess {
					// change will already be in the map
					// we apply changes directly to "ours"
					// instead of to ancestor
					change = types.ValueChanged{}
					processed = true
				}
			}

			if !processed && mergeKey != nil {
				keyNilOrMKLess := key == nil
				if !keyNilOrMKLess {
					keyNilOrMKLess, err = mergeKey.Less(vrw.Format(), key)
					if err != nil {
						return err
					}
				}

				if keyNilOrMKLess {
					err = applyChange(ctx, sch, tblEdit, rows, stats, mergeChange)
					if err != nil {
						return err
					}
					mergeChange = types.ValueChanged{}
					processed = true
				}
			}

			if !processed {
				r, mergeRow, ancRow := change.NewValue, mergeChange.NewValue, change.OldValue
				rowMergeResult, err := rowMerge(ctx, vrw.Format(), sch, r, mergeRow, ancRow)
				if err != nil {
					return err
				}

				if rowMergeResult.isConflict {
					stats.Conflicts++
					conflictTuple, err := conflict.NewConflict(ancRow, r, mergeRow).ToNomsList(vrw)
					if err != nil {
						return err
					}

					err = addConflict(conflictValChan, sm.Done(), key, conflictTuple)
					if err != nil {
						return err
					}
				} else {
					vc := types.ValueChanged{ChangeType: change.ChangeType, Key: key, NewValue: rowMergeResult.mergedRow}
					if rowMergeResult.didCellMerge {
						vc.OldValue = r
					} else {
						vc.OldValue = ancRow
					}
					err = applyChange(ctx, sch, tblEdit, rows, stats, vc)
					if err != nil {
						return err
					}
				}

				change = types.ValueChanged{}
				mergeChange = types.ValueChanged{}
			}
		}

		return nil
	})

	var conflicts types.Map
	eg.Go(func() error {
		var err error
		// |sm|'s errgroup is a child of |eg|
		// so we must wait here, before |eg| finishes
		conflicts, err = sm.Wait()
		return err
	})

	if err := eg.Wait(); err != nil {
		return nil, types.EmptyMap, nil, err
	}

	mergedTable, err := tblEdit.Table(ctx)
	if err != nil {
		return nil, types.EmptyMap, nil, err
	}

	return mergedTable, conflicts, stats, nil
}

func addConflict(conflictChan chan types.Value, done <-chan struct{}, key types.Value, value types.Tuple) error {
	select {
	case conflictChan <- key:
	case <-done:
		return context.Canceled
	}
	select {
	case conflictChan <- value:
	case <-done:
		return context.Canceled
	}
	return nil
}

func applyNomsPkChange(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, rowData types.Map, stats *MergeStats, change types.ValueChanged) error {
	switch change.ChangeType {
	case types.DiffChangeAdded:
		newRow, err := row.FromNoms(sch, change.Key.(types.Tuple), change.NewValue.(types.Tuple))
		if err != nil {
			return err
		}
		// TODO(andy): because we apply changes to "ours" instead of ancestor
		// we have to check for duplicate primary key errors here.
		val, ok, err := rowData.MaybeGet(ctx, change.Key)
		if err != nil {
			return err
		} else if ok {
			oldRow, err := row.FromNoms(sch, change.Key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			err = tableEditor.UpdateRow(ctx, oldRow, newRow, handleTableEditorDuplicateErr)
			if err != nil {
				err = applyPkChangeUnqErr(ctx, err, change.Key.(types.Tuple), change.NewValue.(types.Tuple), tableEditor)
				if err != nil {
					return err
				}
			}
		} else {
			err = tableEditor.InsertRow(ctx, newRow, handleTableEditorDuplicateErr)
			if err != nil {
				err = applyPkChangeUnqErr(ctx, err, change.Key.(types.Tuple), change.NewValue.(types.Tuple), tableEditor)
				if err != nil {
					return err
				}
			}
		}
		stats.Adds++
	case types.DiffChangeModified:
		oldRow, err := row.FromNoms(sch, change.Key.(types.Tuple), change.OldValue.(types.Tuple))
		if err != nil {
			return err
		}
		newRow, err := row.FromNoms(sch, change.Key.(types.Tuple), change.NewValue.(types.Tuple))
		if err != nil {
			return err
		}
		err = tableEditor.UpdateRow(ctx, oldRow, newRow, handleTableEditorDuplicateErr)
		if err != nil {
			err = applyPkChangeUnqErr(ctx, err, change.Key.(types.Tuple), change.NewValue.(types.Tuple), tableEditor)
			if err != nil {
				return err
			}
		}
		stats.Modifications++
	case types.DiffChangeRemoved:
		key := change.Key.(types.Tuple)
		value := change.OldValue.(types.Tuple)
		tv, err := row.TaggedValuesFromTupleKeyAndValue(key, value)
		if err != nil {
			return err
		}

		err = tableEditor.DeleteByKey(ctx, key, tv)
		if err != nil {
			return err
		}

		stats.Deletes++
	}

	return nil
}

// applyPkChangeUnqErr handles unique key errors for the applyNomsPkChange if an error is returned from a table editor.
// If the given error is not a unique key error, then it is returned as-is. Otherwise, it is added to the constraint
// violations map if applicable.
func applyPkChangeUnqErr(ctx context.Context, err error, k, v types.Tuple, tableEditor editor.TableEditor) error {
	if uke, ok := err.(uniqueKeyError); ok {
		sch := tableEditor.Schema()
		schCols := sch.GetAllCols()
		idx := sch.Indexes().GetByName(uke.indexName)
		idxTags := idx.IndexedColumnTags()
		colNames := make([]string, len(idxTags))
		for i, tag := range idxTags {
			if col, ok := schCols.TagToCol[tag]; !ok {
				return fmt.Errorf("unique key '%s' references tag '%d' on table '%s' but it cannot be found",
					idx.Name(), tag, tableEditor.Name())
			} else {
				colNames[i] = col.Name
			}
		}
		jsonStr := fmt.Sprintf(`{`+
			`"Name":"%s",`+
			`"Columns":["%s"]`+
			`}`,
			uke.indexName,
			strings.Join(colNames, `','`))

		var doc interface{}
		if err := json.Unmarshal([]byte(jsonStr), &doc); err != nil {
			return err
		}
		sqlDoc := sql.JSONDocument{Val: doc}
		nomsJson, err := json2.NomsJSONFromJSONValue(ctx, tableEditor.ValueReadWriter(), sqlDoc)
		if err != nil {
			return err
		}
		cvKey, cvVal, err := toConstraintViolationRow(ctx, cvType_UniqueIndex, types.JSON(nomsJson), k, v)
		if err != nil {
			return err
		}
		err = tableEditor.SetConstraintViolation(ctx, cvKey, cvVal)
		if err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}

func applyKeylessChange(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, _ types.Map, stats *MergeStats, change types.ValueChanged) (err error) {
	apply := func(ch types.ValueChanged) error {
		switch ch.ChangeType {
		case types.DiffChangeAdded:
			newRow, err := row.FromNoms(sch, ch.Key.(types.Tuple), ch.NewValue.(types.Tuple))
			if err != nil {
				return err
			}
			err = tableEditor.InsertRow(ctx, newRow, nil)
			if err != nil {
				return err
			}
			stats.Adds++
		case types.DiffChangeModified:
			oldRow, err := row.FromNoms(sch, ch.Key.(types.Tuple), ch.OldValue.(types.Tuple))
			if err != nil {
				return err
			}
			newRow, err := row.FromNoms(sch, ch.Key.(types.Tuple), ch.NewValue.(types.Tuple))
			if err != nil {
				return err
			}
			err = tableEditor.UpdateRow(ctx, oldRow, newRow, nil)
			if err != nil {
				return err
			}
			stats.Modifications++
		case types.DiffChangeRemoved:
			key := change.Key.(types.Tuple)
			value := change.OldValue.(types.Tuple)
			tv, err := row.TaggedValuesFromTupleKeyAndValue(key, value)
			if err != nil {
				return err
			}

			err = tableEditor.DeleteByKey(ctx, key, tv)
			if err != nil {
				return err
			}

			stats.Deletes++
		}
		return nil
	}

	var card uint64
	change, card, err = convertValueChanged(change)
	if err != nil {
		return err
	}

	for card > 0 {
		if err = apply(change); err != nil {
			return err
		}
		card--
	}
	return nil
}

func convertValueChanged(vc types.ValueChanged) (types.ValueChanged, uint64, error) {
	var oldCard uint64
	if vc.OldValue != nil {
		v, err := vc.OldValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return vc, 0, err
		}
		oldCard = uint64(v.(types.Uint))
	}

	var newCard uint64
	if vc.NewValue != nil {
		v, err := vc.NewValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return vc, 0, err
		}
		newCard = uint64(v.(types.Uint))
	}

	switch vc.ChangeType {
	case types.DiffChangeRemoved:
		return vc, oldCard, nil

	case types.DiffChangeAdded:
		return vc, newCard, nil

	case types.DiffChangeModified:
		delta := int64(newCard) - int64(oldCard)
		if delta > 0 {
			vc.ChangeType = types.DiffChangeAdded
			vc.OldValue = nil
			return vc, uint64(delta), nil
		} else if delta < 0 {
			vc.ChangeType = types.DiffChangeRemoved
			vc.NewValue = nil
			return vc, uint64(-delta), nil
		} else {
			panic(fmt.Sprintf("diff with delta = 0 for key: %s", vc.Key.HumanReadableString()))
		}
	default:
		return vc, 0, fmt.Errorf("unexpected DiffChange type %d", vc.ChangeType)
	}
}

// pkRowMerge returns the merged value, if a cell-wise merge was performed, and whether a conflict occurred
func nomsPkRowMerge(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (rowMergeResult, error) {
	var baseVals row.TaggedValues
	if baseRow == nil {
		if r.Equals(mergeRow) {
			// same row added to both
			return rowMergeResult{r, false, false}, nil
		}
	} else if r == nil && mergeRow == nil {
		// same row removed from both
		return rowMergeResult{nil, false, false}, nil
	} else if r == nil || mergeRow == nil {
		// removed from one and modified in another
		return rowMergeResult{nil, false, true}, nil
	} else {
		var err error
		baseVals, err = row.ParseTaggedValues(baseRow.(types.Tuple))

		if err != nil {
			return rowMergeResult{}, err
		}
	}

	rowVals, err := row.ParseTaggedValues(r.(types.Tuple))
	if err != nil {
		return rowMergeResult{}, err
	}

	mergeVals, err := row.ParseTaggedValues(mergeRow.(types.Tuple))
	if err != nil {
		return rowMergeResult{}, err
	}

	var didMerge bool
	processTagFunc := func(tag uint64) (resultVal types.Value, isConflict bool) {
		baseVal, _ := baseVals.Get(tag)
		val, _ := rowVals.Get(tag)
		mergeVal, _ := mergeVals.Get(tag)

		if valutil.NilSafeEqCheck(val, mergeVal) {
			return val, false
		}

		if baseRow == nil {
			// Conflicting insert
			return nil, true
		}

		modified := !valutil.NilSafeEqCheck(val, baseVal)
		mergeModified := !valutil.NilSafeEqCheck(mergeVal, baseVal)
		switch {
		case modified && mergeModified:
			return nil, true
		case modified:
			didMerge = true
			return val, false
		default:
			didMerge = true
			return mergeVal, false
		}
	}

	resultVals := make(row.TaggedValues)

	var isConflict bool
	err = sch.GetNonPKCols().Iter(func(tag uint64, _ schema.Column) (stop bool, err error) {
		var val types.Value
		val, isConflict = processTagFunc(tag)
		resultVals[tag] = val

		return isConflict, nil
	})

	if err != nil {
		return rowMergeResult{}, err
	}

	if isConflict {
		return rowMergeResult{nil, false, true}, nil
	}

	tpl := resultVals.NomsTupleForNonPKCols(nbf, sch.GetNonPKCols())
	v, err := tpl.Value(ctx)

	if err != nil {
		return rowMergeResult{}, err
	}

	return rowMergeResult{v, didMerge, false}, nil
}

func keylessRowMerge(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, val, mergeVal, ancVal types.Value) (rowMergeResult, error) {
	// both sides of the merge produced a diff for this key,
	// so we always throw a conflict
	return rowMergeResult{nil, false, true}, nil
}

func mergeAutoIncrementValues(ctx context.Context, tbl, otherTbl, resultTbl *doltdb.Table) (*doltdb.Table, error) {
	// only need to check one table, no PK changes yet
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	if !schema.HasAutoIncrement(sch) {
		return resultTbl, nil
	}

	autoVal, err := tbl.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}
	mergeAutoVal, err := otherTbl.GetAutoIncrementValue(ctx)
	if err != nil {
		return nil, err
	}
	if autoVal < mergeAutoVal {
		autoVal = mergeAutoVal
	}
	return resultTbl.SetAutoIncrementValue(ctx, autoVal)
}

func MergeCommits(ctx context.Context, commit, mergeCommit *doltdb.Commit, opts editor.Options) (*doltdb.RootValue, map[string]*MergeStats, error) {
	ancCommit, err := doltdb.GetCommitAncestor(ctx, commit, mergeCommit)
	if err != nil {
		return nil, nil, err
	}

	ourRoot, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	theirRoot, err := mergeCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	ancRoot, err := ancCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	return MergeRoots(ctx, ourRoot, theirRoot, ancRoot, opts)
}

func MergeRoots(ctx context.Context, ourRoot, theirRoot, ancRoot *doltdb.RootValue, opts editor.Options) (*doltdb.RootValue, map[string]*MergeStats, error) {
	tblNames, err := doltdb.UnionTableNames(ctx, ourRoot, theirRoot)

	if err != nil {
		return nil, nil, err
	}

	tblToStats := make(map[string]*MergeStats)

	newRoot := ourRoot

	optsWithFKChecks := opts
	optsWithFKChecks.ForeignKeyChecksDisabled = true

	// Merge tables one at a time. This is done based on name, so will work badly for things like table renames.
	// TODO: merge based on a more durable table identity that persists across renames
	merger := NewMerger(ctx, ourRoot, theirRoot, ancRoot, ourRoot.VRW())
	for _, tblName := range tblNames {
		mergedTable, stats, err := merger.MergeTable(ctx, tblName, opts)
		if err != nil {
			return nil, nil, err
		}

		if mergedTable != nil {
			tblToStats[tblName] = stats

			newRoot, err = newRoot.PutTable(ctx, tblName, mergedTable)
			if err != nil {
				return nil, nil, err
			}
			continue
		}

		newRootHasTable, err := newRoot.HasTable(ctx, tblName)
		if err != nil {
			return nil, nil, err
		}

		if newRootHasTable {
			// Merge root deleted this table
			tblToStats[tblName] = &MergeStats{Operation: TableRemoved}

			newRoot, err = newRoot.RemoveTables(ctx, false, false, tblName)
			if err != nil {
				return nil, nil, err
			}

		} else {
			// This is a deleted table that the merge root still has
			if stats.Operation != TableRemoved {
				panic(fmt.Sprintf("Invalid merge state for table %s. This is a bug.", tblName))
			}
			// Nothing to update, our root already has the table deleted
		}
	}

	mergedFKColl, conflicts, err := ForeignKeysMerge(ctx, newRoot, ourRoot, theirRoot, ancRoot)
	if err != nil {
		return nil, nil, err
	}
	if len(conflicts) > 0 {
		return nil, nil, fmt.Errorf("foreign key conflicts")
	}

	newRoot, err = newRoot.PutForeignKeyCollection(ctx, mergedFKColl)
	if err != nil {
		return nil, nil, err
	}

	newRoot, err = newRoot.UpdateSuperSchemasFromOther(ctx, tblNames, theirRoot)
	if err != nil {
		return nil, nil, err
	}

	newRoot, _, err = AddConstraintViolations(ctx, newRoot, ancRoot, nil)
	if err != nil {
		return nil, nil, err
	}

	err = calculateViolationStats(ctx, newRoot, tblToStats)
	if err != nil {
		return nil, nil, err
	}

	return newRoot, tblToStats, nil
}

func calculateViolationStats(ctx context.Context, root *doltdb.RootValue, tblToStats map[string]*MergeStats) error {

	for tblName, stats := range tblToStats {
		tbl, ok, err := root.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if ok {
			cvMap, err := tbl.GetConstraintViolations(ctx)
			if err != nil {
				return err
			}
			stats.ConstraintViolations = int(cvMap.Len())
		}
	}
	return nil
}

// MayHaveConstraintViolations returns whether the given roots may have constraint violations. For example, a fast
// forward merge that does not involve any tables with foreign key constraints or check constraints will not be able
// to generate constraint violations. Unique key constraint violations would be caught during the generation of the
// merged root, therefore it is not a factor for this function.
func MayHaveConstraintViolations(ctx context.Context, ancestor, merged *doltdb.RootValue) (bool, error) {
	ancTables, err := ancestor.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	mergedTables, err := merged.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	fkColl, err := merged.GetForeignKeyCollection(ctx)
	if err != nil {
		return false, err
	}
	tablesInFks := fkColl.Tables()
	for tblName := range tablesInFks {
		if ancHash, ok := ancTables[tblName]; !ok {
			// If a table used in a foreign key is new then it's treated as a change
			return true, nil
		} else if mergedHash, ok := mergedTables[tblName]; !ok {
			return false, fmt.Errorf("foreign key uses table '%s' but no hash can be found for this table", tblName)
		} else if !ancHash.Equal(mergedHash) {
			return true, nil
		}
	}
	return false, nil
}

func GetTablesInConflict(ctx context.Context, roots doltdb.Roots) (
	workingInConflict, stagedInConflict, headInConflict []string,
	err error,
) {
	headInConflict, err = roots.Head.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	stagedInConflict, err = roots.Staged.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	workingInConflict, err = roots.Working.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return workingInConflict, stagedInConflict, headInConflict, err
}

func GetTablesWithConstraintViolations(ctx context.Context, roots doltdb.Roots) (
	workingViolations, stagedViolations, headViolations []string,
	err error,
) {
	headViolations, err = roots.Head.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	stagedViolations, err = roots.Staged.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	workingViolations, err = roots.Working.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return workingViolations, stagedViolations, headViolations, err
}

func GetDocsInConflict(ctx context.Context, workingRoot *doltdb.RootValue, docs doltdocs.Docs) (*diff.DocDiffs, error) {
	return diff.NewDocDiffs(ctx, workingRoot, nil, docs)
}

func MergeWouldStompChanges(ctx context.Context, roots doltdb.Roots, mergeCommit *doltdb.Commit) ([]string, map[string]hash.Hash, error) {
	mergeRoot, err := mergeCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	headTableHashes, err := roots.Head.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	workingTableHashes, err := roots.Working.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeTableHashes, err := mergeRoot.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	headWorkingDiffs := diffTableHashes(headTableHashes, workingTableHashes)
	mergedHeadDiffs := diffTableHashes(headTableHashes, mergeTableHashes)

	stompedTables := make([]string, 0, len(headWorkingDiffs))
	for tName, _ := range headWorkingDiffs {
		if _, ok := mergedHeadDiffs[tName]; ok {
			// even if the working changes match the merge changes, don't allow (matches git behavior).
			stompedTables = append(stompedTables, tName)
		}
	}

	return stompedTables, headWorkingDiffs, nil
}

func diffTableHashes(headTableHashes, otherTableHashes map[string]hash.Hash) map[string]hash.Hash {
	diffs := make(map[string]hash.Hash)
	for tName, hh := range headTableHashes {
		if h, ok := otherTableHashes[tName]; ok {
			if h != hh {
				// modification
				diffs[tName] = h
			}
		} else {
			// deletion
			diffs[tName] = hash.Hash{}
		}
	}

	for tName, h := range otherTableHashes {
		if _, ok := headTableHashes[tName]; !ok {
			// addition
			diffs[tName] = h
		}
	}

	return diffs
}
