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
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

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
func mergeTableData(
	ctx context.Context,
	vrw types.ValueReadWriter,
	ns tree.NodeStore,
	tblName string,
	postMergeSchema, rootSchema, mergeSchema, ancSchema schema.Schema,
	tbl, mergeTbl, updatedTbl *doltdb.Table,
	ancRows durable.Index,
	ancIndexSet durable.IndexSet,
	mergeRootIsh hash.Hash,
	ancRootIsh hash.Hash) (*doltdb.Table, *MergeStats, error) {
	group, gCtx := errgroup.WithContext(ctx)

	indexEdits := make(chan indexEdit, 128)
	conflicts := make(chan confVals, 128)
	var mergedData durable.Index

	group.Go(func() error {
		var err error
		updatedTbl, mergedData, err = mergeProllyRowData(gCtx, postMergeSchema, rootSchema, mergeSchema, ancSchema, tbl, mergeTbl, updatedTbl, ancRows, indexEdits, conflicts)
		if err != nil {
			return err
		}
		defer close(indexEdits)
		defer close(conflicts)
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
		updatedRootIndexSet, updatedMergeIndexSet, err = updateProllySecondaryIndexes(gCtx, indexEdits, rootSchema, mergeSchema, tbl, mergeTbl, rootIndexSet, mergeIndexSet)
		return err
	})

	artIdx, err := updatedTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, nil, err
	}
	artM := durable.ProllyMapFromArtifactIndex(artIdx)
	artifactEditor := artM.Editor()

	var p conflictProcessor
	if can, err := isNewConflictsCompatible(ctx, tbl, tblName, ancSchema, rootSchema, mergeSchema); err != nil {
		return nil, nil, err
	} else if can {
		p, err = newInsertingProcessor(mergeRootIsh, ancRootIsh)
		if err != nil {
			return nil, nil, err
		}
	} else {
		p = abortingProcessor{}
	}

	group.Go(func() error {
		return p.process(gCtx, conflicts, artifactEditor)
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

	updatedTbl, err = mergeProllySecondaryIndexes(
		ctx,
		vrw,
		ns,
		postMergeSchema, rootSchema, mergeSchema, ancSchema,
		mergedData,
		tbl, mergeTbl, updatedTbl,
		ancIndexSet,
		artifactEditor,
		mergeRootIsh,
		tblName)
	if err != nil {
		return nil, nil, err
	}

	artifactMap, err := artifactEditor.Flush(ctx)
	if err != nil {
		return nil, nil, err
	}
	artIdx = durable.ArtifactIndexFromProllyMap(artifactMap)

	updatedTbl, err = updatedTbl.SetArtifacts(ctx, artIdx)
	if err != nil {
		return nil, nil, err
	}

	// TODO (dhruv): populate Adds, Deletes, Modifications
	stats := &MergeStats{Operation: TableModified}
	return updatedTbl, stats, nil
}

func mergeTableArtifacts(ctx context.Context, tblName string, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table) (*doltdb.Table, error) {
	artsIdx, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	mergeArtsIdx, err := mergeTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	ancArtsIdx, err := ancTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	arts := durable.ProllyMapFromArtifactIndex(artsIdx)
	mergeArts := durable.ProllyMapFromArtifactIndex(mergeArtsIdx)
	ancArts := durable.ProllyMapFromArtifactIndex(ancArtsIdx)

	var keyCollision bool
	mergedArts, err := prolly.MergeArtifactMaps(ctx, arts, mergeArts, ancArts, func(left, right tree.Diff) (tree.Diff, bool) {
		if left.Type == right.Type && bytes.Equal(left.To, right.To) {
			// convergent edit
			return left, true
		}
		keyCollision = true
		return tree.Diff{}, false
	})
	if err != nil {
		return nil, err
	}
	idx := durable.ArtifactIndexFromProllyMap(mergedArts)

	if keyCollision {
		return nil, fmt.Errorf("encountered a key collision when merging the artifacts for table %s", tblName)
	}

	updatedTable, err := tableToUpdate.SetArtifacts(ctx, idx)
	if err != nil {
		return nil, err
	}

	return updatedTable, nil
}

// returns true if newly generated conflicts are compatible with existing conflicts in the artifact table.
func isNewConflictsCompatible(ctx context.Context, tbl *doltdb.Table, tblName string, base, ours, theirs schema.Schema) (bool, error) {
	has, err := tbl.HasConflicts(ctx)
	if err != nil {
		return false, err
	}

	if !has {
		return true, nil
	}

	eBase, eOurs, eTheirs, err := tbl.GetConflictSchemas(ctx, tblName)
	if err != nil {
		return false, err
	}

	if schema.ColCollsAreEqual(eBase.GetAllCols(), base.GetAllCols()) &&
		schema.ColCollsAreEqual(eOurs.GetAllCols(), ours.GetAllCols()) &&
		schema.ColCollsAreEqual(eTheirs.GetAllCols(), theirs.GetAllCols()) {
		return true, nil
	}

	return false, nil
}

// mergeProllyRowData merges the primary row table indexes of |tbl|, |mergeTbl|,
// and |ancTbl|. It stores the merged row data into |tableToUpdate| and returns the new value along with the row data.
func mergeProllyRowData(ctx context.Context, postMergeSchema, rootSch, mergeSch, ancSch schema.Schema, tbl, mergeTbl, tableToUpdate *doltdb.Table, ancRows durable.Index, indexEdits chan indexEdit, conflicts chan confVals) (*doltdb.Table, durable.Index, error) {
	rootR, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	mergeR, err := mergeTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	rootRP := durable.ProllyMapFromIndex(rootR)
	mergeRP := durable.ProllyMapFromIndex(mergeR)
	ancRP := durable.ProllyMapFromIndex(ancRows)

	m := durable.ProllyMapFromIndex(rootR)
	vMerger := newValueMerger(postMergeSchema, rootSch, mergeSch, ancSch, m.Pool())
	keyless := schema.IsKeyless(postMergeSchema)

	mergedRP, err := prolly.MergeMaps(ctx, rootRP, mergeRP, ancRP, func(left, right tree.Diff) (tree.Diff, bool) {
		if left.Type == right.Type && bytes.Equal(left.To, right.To) {
			if keyless {
				// convergent edits are conflicts for keyless tables
				_ = sendConflict(ctx, conflicts, indexEdits, left, right)
				return tree.Diff{}, false
			}
			return left, true
		}

		merged, isConflict := vMerger.tryMerge(val.Tuple(left.To), val.Tuple(right.To), val.Tuple(left.From))
		if isConflict {
			_ = sendConflict(ctx, conflicts, indexEdits, left, right)
			return tree.Diff{}, false
		}

		d := tree.Diff{
			Type: tree.ModifiedDiff,
			Key:  left.Key,
			From: left.From,
			To:   tree.Item(merged),
		}

		select {
		case indexEdits <- cellWiseMergeEdit{left, right, d}:
			break
		case <-ctx.Done():
			return tree.Diff{}, false
		}

		return d, true
	})
	if err != nil {
		return nil, nil, err
	}

	updatedTbl, err := tableToUpdate.UpdateRows(ctx, durable.IndexFromProllyMap(mergedRP))
	if err != nil {
		return nil, nil, err
	}

	return updatedTbl, durable.IndexFromProllyMap(mergedRP), nil
}

func sendConflict(ctx context.Context, confs chan confVals, edits chan indexEdit, left, right tree.Diff) error {
	c := confVals{
		key:      val.Tuple(left.Key),
		ourVal:   val.Tuple(left.To),
		theirVal: val.Tuple(right.To),
		baseVal:  val.Tuple(left.From),
	}
	select {
	case confs <- c:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Reset the change on the right
	e := conflictEdit{
		right: right,
	}
	select {
	case edits <- e:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
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
		vD:           shim.ValueDescriptorFromSchema(merged),
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

type conflictProcessor interface {
	process(ctx context.Context, conflictChan chan confVals, artEditor prolly.ArtifactsEditor) error
}

type insertingProcessor struct {
	theirRootIsh hash.Hash
	jsonMetaData []byte
}

func newInsertingProcessor(theirRootIsh, baseRootIsh hash.Hash) (*insertingProcessor, error) {
	m := prolly.ConflictMetadata{
		BaseRootIsh: baseRootIsh,
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	p := insertingProcessor{
		theirRootIsh: theirRootIsh,
		jsonMetaData: data,
	}
	return &p, nil
}

func (p *insertingProcessor) process(ctx context.Context, conflictChan chan confVals, artEditor prolly.ArtifactsEditor) error {
OUTER:
	for {
		select {
		case conflict, ok := <-conflictChan:
			if !ok {
				break OUTER
			}
			err := artEditor.Add(ctx, conflict.key, p.theirRootIsh, prolly.ArtifactTypeConflict, p.jsonMetaData)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

type abortingProcessor struct{}

func (p abortingProcessor) process(ctx context.Context, conflictChan chan confVals, artEditor prolly.ArtifactsEditor) error {
	select {
	case _, ok := <-conflictChan:
		if !ok {
			break
		}
		return ErrConflictsIncompatible
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
