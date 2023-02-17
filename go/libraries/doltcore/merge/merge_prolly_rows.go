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
	"github.com/dolthub/dolt/go/store/prolly/tree"
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
func mergeTableData(ctx context.Context, tm TableMerger, finalSch schema.Schema, mergeTbl *doltdb.Table) (*doltdb.Table, *MergeStats, error) {
	group, gCtx := errgroup.WithContext(ctx)

	var (
		finalRows durable.Index
		stats     tree.MergeStats

		leftIdxs  durable.IndexSet
		rightIdxs durable.IndexSet
		finalIdxs durable.IndexSet

		indexEdits = make(chan indexEdit, 128)
		conflicts  = make(chan confVals, 128)
	)

	// stage 1: merge clustered indexes and propagate changes from
	//   conflicts and cell-wise merges to secondary indexes

	cp, err := makeConflictProcessor(ctx, tm)
	if err != nil {
		return nil, nil, err
	}

	ai, err := mergeTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, nil, err
	}
	artifacts := durable.ProllyMapFromArtifactIndex(ai).Editor()

	group.Go(func() error {
		return cp.process(gCtx, conflicts, artifacts)
	})

	group.Go(func() (err error) {
		leftIdxs, rightIdxs, err = updateProllySecondaryIndexes(gCtx, tm, indexEdits)
		return err
	})

	group.Go(func() (err error) {
		defer close(indexEdits)
		defer close(conflicts)
		finalRows, stats, err = mergeProllyRowData(gCtx, tm, finalSch, indexEdits, conflicts)
		return err
	})

	err = group.Wait()
	if err != nil {
		return nil, nil, err
	}

	// stage 2: merge the modified versions of the secondary
	//   indexes generated in stage 1

	finalIdxs, err = mergeProllySecondaryIndexes(ctx, tm, leftIdxs, rightIdxs, finalSch, finalRows, artifacts)
	if err != nil {
		return nil, nil, err
	}

	am, err := artifacts.Flush(ctx)
	if err != nil {
		return nil, nil, err
	}
	finalArtifacts := durable.ArtifactIndexFromProllyMap(am)

	// collect merged data in |finalTbl|
	finalTbl, err := mergeTbl.UpdateRows(ctx, finalRows)
	if err != nil {
		return nil, nil, err
	}

	finalTbl, err = finalTbl.SetIndexSet(ctx, finalIdxs)
	if err != nil {
		return nil, nil, err
	}

	finalTbl, err = finalTbl.SetArtifacts(ctx, finalArtifacts)
	if err != nil {
		return nil, nil, err
	}

	s := &MergeStats{
		Operation:     TableModified,
		Adds:          stats.Adds,
		Deletes:       stats.Removes,
		Modifications: stats.Modifications,
	}
	return finalTbl, s, nil
}

func mergeTableArtifacts(ctx context.Context, tm TableMerger, mergeTbl *doltdb.Table) (*doltdb.Table, error) {
	la, err := tm.leftTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	left := durable.ProllyMapFromArtifactIndex(la)

	ra, err := tm.rightTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	right := durable.ProllyMapFromArtifactIndex(ra)

	aa, err := tm.ancTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	anc := durable.ProllyMapFromArtifactIndex(aa)

	var keyCollision bool
	collide := func(l, r tree.Diff) (tree.Diff, bool) {
		if l.Type == r.Type && bytes.Equal(l.To, r.To) {
			return l, true // convergent edit
		}
		keyCollision = true
		return tree.Diff{}, false
	}

	ma, err := prolly.MergeArtifactMaps(ctx, left, right, anc, collide)
	if err != nil {
		return nil, err
	}
	idx := durable.ArtifactIndexFromProllyMap(ma)

	if keyCollision {
		return nil, fmt.Errorf("encountered a key collision when merging the artifacts for table %s", tm.name)
	}

	return mergeTbl.SetArtifacts(ctx, idx)
}

// mergeProllyRowData merges the primary row table indexes of |tbl|, |mergeTbl|,
// and |ancTbl|. It stores the merged row data into |tableToUpdate| and returns the new value along with the row data.
func mergeProllyRowData(
	ctx context.Context,
	tm TableMerger,
	finalSch schema.Schema,
	indexEdits chan indexEdit,
	conflicts chan confVals,
) (durable.Index, tree.MergeStats, error) {

	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, tree.MergeStats{}, err
	}
	leftRows := durable.ProllyMapFromIndex(lr)

	rr, err := tm.rightTbl.GetRowData(ctx)
	if err != nil {
		return nil, tree.MergeStats{}, err
	}
	rightRows := durable.ProllyMapFromIndex(rr)

	ar, err := tm.ancTbl.GetRowData(ctx)
	if err != nil {
		return nil, tree.MergeStats{}, err
	}
	ancRows := durable.ProllyMapFromIndex(ar)

	vMerger := newValueMerger(finalSch, tm.leftSch, tm.rightSch, tm.ancSch, leftRows.Pool())
	keyless := schema.IsKeyless(finalSch)

	mr, stats, err := prolly.MergeMaps(ctx, leftRows, rightRows, ancRows, func(left, right tree.Diff) (tree.Diff, bool) {
		if left.Type == right.Type && bytes.Equal(left.To, right.To) {
			if keyless {
				// convergent edits are conflicts for keyless tables
				d, b, _ := processConflict(ctx, conflicts, indexEdits, left, right)
				return d, b
			}
			return left, true
		}

		merged, isConflict := vMerger.tryMerge(val.Tuple(left.To), val.Tuple(right.To), val.Tuple(left.From))
		if isConflict {
			d, b, _ := processConflict(ctx, conflicts, indexEdits, left, right)
			return d, b
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
		return nil, tree.MergeStats{}, err
	}

	return durable.IndexFromProllyMap(mr), stats, nil
}

func processConflict(ctx context.Context, confs chan confVals, edits chan indexEdit, left, right tree.Diff) (tree.Diff, bool, error) {
	c := confVals{
		key:      val.Tuple(left.Key),
		ourVal:   val.Tuple(left.To),
		theirVal: val.Tuple(right.To),
		baseVal:  val.Tuple(left.From),
	}
	select {
	case confs <- c:
	case <-ctx.Done():
		return tree.Diff{}, false, ctx.Err()
	}
	// Reset the change on the right
	e := conflictEdit{right: right}
	select {
	case edits <- e:
	case <-ctx.Done():
		return tree.Diff{}, false, ctx.Err()
	}
	return tree.Diff{}, false, nil
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
		vD:           merged.GetValueDescriptor(),
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

	if m.vD.Comparator().CompareValues(i, leftCol, rightCol, m.vD.Types[i]) == 0 {
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

	leftModified := m.vD.Comparator().CompareValues(i, leftCol, baseVal, m.vD.Types[i]) != 0
	rightModified := m.vD.Comparator().CompareValues(i, rightCol, baseVal, m.vD.Types[i]) != 0

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
	process(ctx context.Context, conflictChan chan confVals, artEditor *prolly.ArtifactsEditor) error
}

func makeConflictProcessor(ctx context.Context, tm TableMerger) (conflictProcessor, error) {
	has, err := tm.leftTbl.HasConflicts(ctx)
	if err != nil {
		return nil, err
	}
	if !has {
		return newInsertingProcessor(tm.rightSrc, tm.ancestorSrc)
	}

	a, l, r, err := tm.leftTbl.GetConflictSchemas(ctx, tm.name)
	if err != nil {
		return nil, err
	}

	equal := schema.ColCollsAreEqual(a.GetAllCols(), tm.ancSch.GetAllCols()) &&
		schema.ColCollsAreEqual(l.GetAllCols(), tm.leftSch.GetAllCols()) &&
		schema.ColCollsAreEqual(r.GetAllCols(), tm.rightSch.GetAllCols())
	if !equal {
		return abortingProcessor{}, nil
	}

	return newInsertingProcessor(tm.rightSrc, tm.ancestorSrc)
}

type insertingProcessor struct {
	theirRootIsh hash.Hash
	jsonMetaData []byte
}

func newInsertingProcessor(theirRootIsh, baseRootIsh doltdb.Rootish) (*insertingProcessor, error) {
	theirHash, err := theirRootIsh.HashOf()
	if err != nil {
		return nil, err
	}

	baseHash, err := baseRootIsh.HashOf()
	if err != nil {
		return nil, err
	}

	m := prolly.ConflictMetadata{
		BaseRootIsh: baseHash,
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	p := insertingProcessor{
		theirRootIsh: theirHash,
		jsonMetaData: data,
	}
	return &p, nil
}

func (p *insertingProcessor) process(ctx context.Context, conflictChan chan confVals, artEditor *prolly.ArtifactsEditor) error {
	for {
		select {
		case conflict, ok := <-conflictChan:
			if !ok {
				return nil
			}
			err := artEditor.Add(ctx, conflict.key, p.theirRootIsh, prolly.ArtifactTypeConflict, p.jsonMetaData)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type abortingProcessor struct{}

func (p abortingProcessor) process(ctx context.Context, conflictChan chan confVals, _ *prolly.ArtifactsEditor) error {
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
