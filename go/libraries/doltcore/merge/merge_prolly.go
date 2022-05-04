package merge

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type cellWiseMerge struct {
	leftDiff  tree.Diff
	rightDiff tree.Diff
	merged    tree.Diff
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

	var updatedRootIndexSet durable.IndexSet
	var updatedMergeIndexSet durable.IndexSet
	group.Go(func() error {
		var err error
		updatedRootIndexSet, updatedMergeIndexSet, err = updateProllySecondaryIndexes(gCtx, cellWiseMerges, rootSchema, mergeSchema, tbl, mergeTbl, rootIndexSet, mergeIndexSet)
		return err
	})

	err = group.Wait()
	if err != nil {
		return nil, nil, err
	}

	tbl, err = tbl.SetIndexSet(ctx, updatedRootIndexSet)
	if err != nil {
		return nil, nil, err
	}
	mergeTbl, err = mergeTbl.SetIndexSet(ctx, updatedMergeIndexSet)
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

	m := durable.ProllyMapFromIndex(rootR)
	vMerger := newValueMerger(postMergeSchema, rootSch, mergeSch, ancSch, m.Pool())

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

type valueMerger struct {
	numCols                                int
	vD                                     val.TupleDesc
	leftMapping, rightMapping, baseMapping val.OrdinalMapping
	syncPool                               pool.BuffPool
}

func newValueMerger(merged, leftSch, rightSch, baseSch schema.Schema, syncPool pool.BuffPool) *valueMerger {
	n := merged.GetNonPKCols().Size()
	leftMapping := make(val.OrdinalMapping, n)
	rightMapping := make(val.OrdinalMapping, n)
	baseMapping := make(val.OrdinalMapping, n)

	for i, tag := range merged.GetNonPKCols().Tags {
		if j, ok := leftSch.GetNonPKCols().TagToIdx[tag]; ok {
			leftMapping[i] = j
		} else {
			leftMapping[i] = -1
		}
		if j, ok := rightSch.GetNonPKCols().TagToIdx[tag]; ok {
			rightMapping[i] = j
		} else {
			rightMapping[i] = -1
		}
		if j, ok := baseSch.GetNonPKCols().TagToIdx[tag]; ok {
			baseMapping[i] = j
		} else {
			baseMapping[i] = -1
		}
	}

	return &valueMerger{
		numCols:      n,
		vD:           prolly.ValueDescriptorFromSchema(merged),
		leftMapping:  leftMapping,
		rightMapping: rightMapping,
		baseMapping:  baseMapping,
		syncPool:     syncPool,
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

	mergedValues := make([][]byte, m.numCols)
	for i := 0; i < m.numCols; i++ {
		v, isConflict := m.processColumn(i, left, right, base)
		if isConflict {
			return nil, true
		}
		mergedValues[i] = v
	}

	return val.NewTuple(m.syncPool, mergedValues...), false
}

// processColumn returns the merged value of column |i| of the merged schema,
// based on the |left|, |right|, and |base| schema.
func (m *valueMerger) processColumn(i int, left, right, base val.Tuple) ([]byte, bool) {
	// missing columns are coerced into NULL column values
	var leftCol []byte
	if l := m.leftMapping[i]; l != -1 {
		leftCol = left.GetField(l)
	}
	var rightCol []byte
	if r := m.rightMapping[i]; r != -1 {
		rightCol = right.GetField(r)
	}

	if m.vD.Comparator().CompareValues(leftCol, rightCol, m.vD.Types[i]) == 0 {
		return leftCol, false
	}

	if base == nil {
		// Conflicting insert
		return nil, true
	}

	var baseVal []byte
	if b := m.baseMapping[i]; b != -1 {
		baseVal = base.GetField(b)
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

// Given cellWiseMerge's sent on |cellWiseChan|, update the secondary indexes in
// |rootIndexSet| and |mergeIndexSet| such that when the index sets are merged,
// they produce entries consistent with the cell-wise merges. The updated
// |rootIndexSet| and |mergeIndexSet| are returned.
func updateProllySecondaryIndexes(
	ctx context.Context,
	cellWiseChan chan cellWiseMerge,
	rootSchema, mergeSchema schema.Schema,
	tbl, mergeTbl *doltdb.Table,
	rootIndexSet, mergeIndexSet durable.IndexSet) (durable.IndexSet, durable.IndexSet, error) {

	rootIdxs, err := getMutableSecondaryIdxs(ctx, rootSchema, tbl)
	if err != nil {
		return nil, nil, err
	}
	mergeIdxs, err := getMutableSecondaryIdxs(ctx, mergeSchema, mergeTbl)
	if err != nil {
		return nil, nil, err
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
				return nil, nil, err
			}
		}
		for _, idx := range mergeIdxs {
			// Update corresponding idx entry to merged value in right
			err = idx.UpdateEntry(ctx, val.Tuple(m.rightDiff.Key), val.Tuple(m.rightDiff.To), val.Tuple(m.merged.To))
			if err != nil {
				return nil, nil, err
			}
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	persistIndexMuts := func(indexSet durable.IndexSet, idxs []MutableSecondaryIdx) (durable.IndexSet, error) {
		for _, idx := range idxs {
			m, err := idx.Map(ctx)
			if err != nil {
				return nil, err
			}
			indexSet, err = indexSet.PutIndex(ctx, idx.Name, durable.IndexFromProllyMap(m))
			if err != nil {
				return nil, err
			}
		}

		return indexSet, nil
	}

	updatedRootIndexSet, err := persistIndexMuts(rootIndexSet, rootIdxs)
	if err != nil {
		return nil, nil, err
	}

	updatedMergeIndexSet, err := persistIndexMuts(mergeIndexSet, mergeIdxs)
	if err != nil {
		return nil, nil, err
	}

	return updatedRootIndexSet, updatedMergeIndexSet, nil
}

// getMutableSecondaryIdxs returns a MutableSecondaryIdx for each secondary index
// defined in |schema| and |tbl|.
func getMutableSecondaryIdxs(ctx context.Context, schema schema.Schema, tbl *doltdb.Table) ([]MutableSecondaryIdx, error) {
	indexSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	mods := make([]MutableSecondaryIdx, schema.Indexes().Count())
	for i, index := range schema.Indexes().AllIndexes() {
		idx, err := indexSet.GetIndex(ctx, schema, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)

		mods[i] = NewMutableSecondaryIdx(m, schema, index, m.Pool())
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
