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
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/message"
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
	iter, err := threeWayDiffer(ctx, tm, finalSch)
	if err != nil {
		return nil, nil, err
	}

	conflicts, err := newConflictMerger(ctx, tm, mergeTbl)
	if err != nil {
		return nil, nil, err
	}
	pri, err := newPrimaryMerger(ctx, tm)
	if err != nil {
		return nil, nil, err
	}
	sec, err := newSecondaryMerger(ctx, tm, finalSch)
	if err != nil {
		return nil, nil, err
	}

	s := &MergeStats{
		Operation: TableModified,
	}
	for {
		diff, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, nil, err
		}

		switch diff.Op {
		case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
			s.Conflicts++
			err = conflicts.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightAdd:
			s.Adds++
			err = pri.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightModify:
			s.Modifications++
			err = pri.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightDelete:
			s.Deletes++
			err = pri.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpDivergentModifyResolved:
			s.Modifications++
			err = pri.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff)
			if err != nil {
				return nil, nil, err
			}
		default:
		}
	}

	finalRows, err := pri.finalize(ctx)
	leftIdxs, rightIdxs, err := sec.finalize(ctx)

	finalIdxs, err := mergeProllySecondaryIndexes(ctx, tm, leftIdxs, rightIdxs, finalSch, finalRows, conflicts.ae)
	if err != nil {
		return nil, nil, err
	}

	finalArtifacts, err := conflicts.finalize(ctx)

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

	return finalTbl, s, nil
}

func threeWayDiffer(ctx context.Context, tm TableMerger, finalSch schema.Schema) (tree.DiffIter, error) {
	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	leftRows := durable.ProllyMapFromIndex(lr)

	rr, err := tm.rightTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	rightRows := durable.ProllyMapFromIndex(rr)

	ar, err := tm.ancTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	ancRows := durable.ProllyMapFromIndex(ar)

	vMerger := newValueMerger(finalSch, tm.leftSch, tm.rightSch, tm.ancSch, leftRows.Pool())

	return tree.NewThreeWayDiffer(ctx, leftRows.NodeStore(), leftRows.Tuples(), rightRows.Tuples(), ancRows.Tuples(), vMerger.tryMerge, vMerger.keyless, leftRows.Tuples().Order)

}

type diffMerger interface {
	merge(context.Context, tree.ThreeWayDiff) error
}

var _ diffMerger = (*conflictMerger)(nil)
var _ diffMerger = (*primaryMerger)(nil)
var _ diffMerger = (*secondaryMerger)(nil)

type conflictMerger struct {
	ae           *prolly.ArtifactsEditor
	rightRootish hash.Hash
	meta         []byte
}

func newConflictMerger(ctx context.Context, tm TableMerger, mergeTbl *doltdb.Table) (*conflictMerger, error) {
	ai, err := mergeTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	ae := durable.ProllyMapFromArtifactIndex(ai).Editor()

	has, err := tm.leftTbl.HasConflicts(ctx)
	if err != nil {
		return nil, err
	}
	if has {
		a, l, r, err := tm.leftTbl.GetConflictSchemas(ctx, tm.name)
		if err != nil {
			return nil, err
		}

		equal := schema.ColCollsAreEqual(a.GetAllCols(), tm.ancSch.GetAllCols()) &&
			schema.ColCollsAreEqual(l.GetAllCols(), tm.leftSch.GetAllCols()) &&
			schema.ColCollsAreEqual(r.GetAllCols(), tm.rightSch.GetAllCols())
		if !equal {
			return nil, ErrConflictsIncompatible
		}
	}

	rightHash, err := tm.rightSrc.HashOf()
	if err != nil {
		return nil, err
	}

	baseHash, err := tm.ancestorSrc.HashOf()
	if err != nil {
		return nil, err
	}

	m := prolly.ConflictMetadata{
		BaseRootIsh: baseHash,
	}
	meta, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return &conflictMerger{
		meta:         meta,
		rightRootish: rightHash,
		ae:           ae,
	}, nil
}

func (m *conflictMerger) merge(ctx context.Context, diff tree.ThreeWayDiff) error {
	switch diff.Op {
	case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict,
		tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify, tree.DiffOpConvergentDelete:
	default:
		return fmt.Errorf("invalid conflict type: %s", diff.Op)
	}
	return m.ae.Add(ctx, diff.Key, m.rightRootish, prolly.ArtifactTypeConflict, m.meta)

}

func (m *conflictMerger) finalize(ctx context.Context) (durable.ArtifactIndex, error) {
	am, err := m.ae.Flush(ctx)
	if err != nil {
		return nil, err
	}
	return durable.ArtifactIndexFromProllyMap(am), nil
}

type primaryMerger struct {
	serializer message.ProllyMapSerializer
	keyDesc    val.TupleDesc
	valDesc    val.TupleDesc
	ns         tree.NodeStore
	root       tree.Node
	cur        *tree.Cursor
	chunker    tree.Chunker
	key, value val.Tuple
}

func newPrimaryMerger(ctx context.Context, tm TableMerger) (*primaryMerger, error) {
	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	leftRows := durable.ProllyMapFromIndex(lr)

	return &primaryMerger{
		serializer: message.NewProllyMapSerializer(leftRows.ValDesc(), leftRows.NodeStore().Pool()),
		keyDesc:    leftRows.KeyDesc(),
		valDesc:    leftRows.ValDesc(),
		ns:         leftRows.NodeStore(),
		root:       leftRows.Node(),
	}, nil
}

func (m *primaryMerger) init(ctx context.Context, key val.Tuple) error {
	if key == nil {
		return nil // no mutations
	}

	var err error
	m.cur, err = tree.NewCursorAtKey(ctx, m.ns, m.root, key, m.keyDesc)
	if err != nil {
		return err
	}

	m.chunker, err = tree.NewChunker(ctx, m.cur.Clone(), 0, m.ns, m.serializer)
	if err != nil {
		return err
	}
	return nil
}

func (m *primaryMerger) merge(ctx context.Context, diff tree.ThreeWayDiff) error {
	var err error
	if m.chunker == nil {
		err = m.init(ctx, diff.Key)
		if err != nil {
			return err
		}
	}
	newKey := diff.Key
	var newValue val.Tuple
	switch diff.Op {
	case tree.DiffOpRightAdd, tree.DiffOpRightModify, tree.DiffOpRightDelete:
		newValue = diff.Right
	case tree.DiffOpDivergentModifyResolved:
		newValue = diff.Merged
	default:
		return fmt.Errorf("unexpected diffOp for editing primary index: %s", diff.Op)
	}

	err = tree.Seek(ctx, m.cur, newKey, m.keyDesc)
	if err != nil {
		return err
	}

	var oldValue tree.Item
	if m.cur.Valid() {
		// Compare mutations |newKey| and |newValue|
		// to the existing pair from the cursor
		if m.keyDesc.Compare(newKey, val.Tuple(m.cur.CurrentKey())) == 0 {
			oldValue = m.cur.CurrentValue()
		}
	}

	if bytes.Equal(newValue, oldValue) {
		// no-op mutations
		m.key = newKey
		return nil
	}

	// move |chkr| to the NextMutation mutation point
	err = m.chunker.AdvanceTo(ctx, m.cur)
	if err != nil {
		return err
	}

	if oldValue == nil {
		err = m.chunker.AddPair(ctx, tree.Item(newKey), tree.Item(newValue))
	} else {
		if newValue != nil {
			err = m.chunker.UpdatePair(ctx, tree.Item(newKey), tree.Item(newValue))
		} else {
			err = m.chunker.DeletePair(ctx, tree.Item(newKey), oldValue)
		}
	}
	return err
}

func (m *primaryMerger) finalize(ctx context.Context) (durable.Index, error) {
	var err error
	var final tree.Node
	if m.chunker != nil {
		final, err = m.chunker.Done(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		final = m.root
	}
	mergedMap := prolly.NewMap(final, m.ns, m.keyDesc, m.valDesc)

	return durable.IndexFromProllyMap(mergedMap), nil
}

type secondaryMerger struct {
	leftSet   durable.IndexSet
	rightSet  durable.IndexSet
	leftMut   []MutableSecondaryIdx
	rightMut  []MutableSecondaryIdx
	notUnique map[string]struct{}
}

func newSecondaryMerger(ctx context.Context, tm TableMerger, finalSch schema.Schema) (*secondaryMerger, error) {
	ls, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	lm, err := GetMutableSecondaryIdxs(ctx, tm.leftSch, ls)
	if err != nil {
		return nil, err
	}

	rs, err := tm.rightTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	rm, err := GetMutableSecondaryIdxs(ctx, tm.rightSch, rs)
	if err != nil {
		return nil, err
	}

	notUnique := make(map[string]struct{})
	for _, idx := range lm {
		schIdx := finalSch.Indexes().GetByName(idx.Name)
		if schIdx != nil && !schIdx.IsUnique() {
			notUnique[idx.Name] = struct{}{}
		}
	}

	return &secondaryMerger{
		leftSet:   ls,
		rightSet:  rs,
		leftMut:   lm,
		rightMut:  rm,
		notUnique: notUnique,
	}, nil
}

func (m *secondaryMerger) merge(ctx context.Context, diff tree.ThreeWayDiff) error {
	var err error
	for _, idx := range m.leftMut {
		// update left to resolved or reset
		if _, ok := m.notUnique[idx.Name]; ok {
			switch diff.Op {
			case tree.DiffOpDivergentModifyResolved:
				err = applyEdit(ctx, idx, diff.Key, diff.Left, diff.Merged)
			case tree.DiffOpRightAdd, tree.DiffOpRightModify, tree.DiffOpRightDelete:
				err = applyEdit(ctx, idx, diff.Key, diff.Base, diff.Right)
			default:
			}
		} else {
			switch diff.Op {
			case tree.DiffOpDivergentModifyResolved:
				err = applyEdit(ctx, idx, diff.Key, diff.Left, diff.Base)
			default:
			}
		}
	}
	for _, idx := range m.rightMut {
		// ignore or update right for resolve/conflict
		if _, ok := m.notUnique[idx.Name]; ok {
			continue
		} else {
			switch diff.Op {
			case tree.DiffOpDivergentModifyResolved:
				// right -> merged
				err = applyEdit(ctx, idx, diff.Key, diff.Right, diff.Merged)
			case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
				// conflict: right -> base
				err = applyEdit(ctx, idx, diff.Key, diff.Right, diff.Base)
			default:
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *secondaryMerger) finalize(ctx context.Context) (durable.IndexSet, durable.IndexSet, error) {
	for _, idx := range m.leftMut {
		idxMap, err := idx.Map(ctx)
		if err != nil {
			return nil, nil, err
		}
		m.leftSet, err = m.leftSet.PutIndex(ctx, idx.Name, durable.IndexFromProllyMap(idxMap))
		if err != nil {
			return nil, nil, err
		}
	}
	for _, idx := range m.rightMut {
		idxMap, err := idx.Map(ctx)
		if err != nil {
			return nil, nil, err
		}
		m.rightSet, err = m.rightSet.PutIndex(ctx, idx.Name, durable.IndexFromProllyMap(idxMap))
		if err != nil {
			return nil, nil, err
		}
	}
	return m.leftSet, m.rightSet, nil
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

	iter, err := tree.NewThreeWayDiffer(ctx, leftRows.NodeStore(), leftRows.Tuples(), rightRows.Tuples(), ancRows.Tuples(), vMerger.tryMerge, vMerger.keyless, leftRows.Tuples().Order)

	stats := tree.MergeStats{}

	conflictsIter := &tree.TeeDiffIter{
		Iter: iter,
		Cb: func(diff tree.ThreeWayDiff) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			switch diff.Op {
			case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
				// TODO: count conflicts?
				sendConflict(conflicts, diff)
				sendIndexReset(indexEdits, diff)
			case tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify, tree.DiffOpConvergentDelete:
				if vMerger.keyless {
					sendConflict(conflicts, diff)
					sendIndexReset(indexEdits, diff)
				}
			case tree.DiffOpDivergentModifyResolved:
				stats.Modifications++
				sendCellWiseMergeEdit(indexEdits, diff)
			case tree.DiffOpRightAdd:
				stats.Adds++
				sendRightEdit(indexEdits, diff)
			case tree.DiffOpRightModify:
				stats.Modifications++
				sendRightEdit(indexEdits, diff)
			case tree.DiffOpRightDelete:
				stats.Removes++
				sendRightEdit(indexEdits, diff)
			default:
			}
			return nil
		},
	}

	editIter := &editsIter{
		iter: conflictsIter,
	}

	serializer := message.NewProllyMapSerializer(leftRows.ValDesc(), leftRows.NodeStore().Pool())
	final, err := tree.ApplyMutations[val.Tuple](ctx, leftRows.NodeStore(), leftRows.Node(), leftRows.Tuples().Order, serializer, editIter)
	mergedMap := prolly.NewMap(final, leftRows.NodeStore(), leftRows.KeyDesc(), leftRows.ValDesc())

	return durable.IndexFromProllyMap(mergedMap), stats, nil
}

func sendRightEdit(edits chan indexEdit, diff tree.ThreeWayDiff) {
	edits <- rightEdit{key: diff.Key, from: diff.Base, to: diff.Right}
}

func sendCellWiseMergeEdit(edits chan indexEdit, diff tree.ThreeWayDiff) {
	edits <- cellWiseMergeEdit{key: diff.Key, lFrom: diff.Base, lTo: diff.Left, rTo: diff.Right, merged: diff.Merged}
}

func sendConflict(confs chan confVals, diff tree.ThreeWayDiff) {
	confs <- confVals{
		key:      diff.Key,
		ourVal:   diff.Left,
		theirVal: diff.Right,
		baseVal:  diff.Base,
	}
}

func sendIndexReset(edits chan indexEdit, diff tree.ThreeWayDiff) {
	edits <- conflictEdit{key: diff.Key, from: diff.Right, to: diff.Base}
}

type editsIter struct {
	iter tree.DiffIter
}

var _ tree.MutationIter = (*editsIter)(nil)

func (i *editsIter) NextMutation(ctx context.Context) (tree.Item, tree.Item, error) {
	for {
		diff, err := i.iter.Next(ctx)
		if err != nil {
			return nil, nil, err
		}
		switch diff.Op {
		case tree.DiffOpRightAdd, tree.DiffOpRightModify, tree.DiffOpRightDelete:
			return tree.Item(diff.Key), tree.Item(diff.Right), nil
		case tree.DiffOpDivergentModifyResolved:
			return tree.Item(diff.Key), tree.Item(diff.Merged), nil
		default:
		}
	}
}

func (i *editsIter) Close() error {
	return i.iter.Close()
}

type valueMerger struct {
	numCols                                int
	vD                                     val.TupleDesc
	leftMapping, rightMapping, baseMapping val.OrdinalMapping
	syncPool                               pool.BuffPool
	keyless                                bool
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
		keyless:      schema.IsKeyless(merged),
	}
}

// tryMerge performs a cell-wise merge given left, right, and base cell value
// tuples. It returns the merged cell value tuple and a bool indicating if a
// conflict occurred. tryMerge should only be called if left and right produce
// non-identical diffs against base.
func (m *valueMerger) tryMerge(left, right, base val.Tuple) (val.Tuple, bool) {
	if m.keyless {
		return nil, false
	}

	if base != nil && (left == nil) != (right == nil) {
		// One row deleted, the other modified
		return nil, false
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
			return nil, false
		}
		mergedValues[i] = v
	}

	return val.NewTuple(m.syncPool, mergedValues...), true
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
