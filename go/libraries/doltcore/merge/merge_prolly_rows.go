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

	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/skip"
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

	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	leftRows := durable.ProllyMapFromIndex(lr)

	ai, err := mergeTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, nil, err
	}
	ae := durable.ProllyMapFromArtifactIndex(ai).Editor()

	keyless := schema.IsKeyless(tm.leftSch)

	pri, err := newPrimaryMerger(leftRows)
	if err != nil {
		return nil, nil, err
	}
	sec, err := newSecondaryMerger(ctx, tm, finalSch)
	if err != nil {
		return nil, nil, err
	}
	conflicts, err := newConflictMerger(ctx, tm, ae)
	if err != nil {
		return nil, nil, err
	}
	// validator shares editor with conflict merge
	uniq, err := newUniqAddValidator(ctx, finalSch, leftRows, ae, tm)
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
			cnt, err := uniq.valid(ctx, diff.Op, diff.Key, diff.Right)
			if err != nil {
				return nil, nil, err
			}
			s.Conflicts += cnt
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
			cnt, err := uniq.valid(ctx, diff.Op, diff.Key, diff.Right)
			if err != nil {
				return nil, nil, err
			}
			s.Conflicts += cnt
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
			cnt, err := uniq.valid(ctx, diff.Op, diff.Key, diff.Merged)
			if err != nil {
				return nil, nil, err
			}
			s.Conflicts += cnt
		case tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify, tree.DiffOpConvergentDelete:
			if keyless {
				s.Conflicts++
				err = conflicts.merge(ctx, diff)
				if err != nil {
					return nil, nil, err
				}
			}
		default:
		}
	}

	finalRows, err := pri.finalize(ctx)
	if err != nil {
		return nil, nil, err
	}

	leftIdxs, rightIdxs, err := sec.finalize(ctx)
	if err != nil {
		return nil, nil, err
	}

	newUniq, err := uniq.finalize(ctx)
	if err != nil {
		return nil, nil, err
	}
	s.Conflicts += newUniq

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

func threeWayDiffer(ctx context.Context, tm TableMerger, finalSch schema.Schema) (*tree.ThreeWayDiffer[val.Tuple, val.TupleDesc], error) {
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

const uniqAddValidatorPendingSize = 650_000

// uniqAddValidator checks whether new additions from the merge-right
// duplicate secondary index entries.
type uniqAddValidator struct {
	name         string
	rightRootish doltdb.Rootish
	ae           *prolly.ArtifactsEditor
	states       []*validateIndexState
	leftRows     prolly.Map
	leftSch      schema.Schema
	primaryKD    val.TupleDesc
	primaryKB    *val.TupleBuilder
	batchSize    int
	pkLen        int
}

// validateIndexState carries the state required to validate
// a single unique index.
type validateIndexState struct {
	index     schema.Index
	leftMap   prolly.Map
	vInfo     []byte
	pkMapping val.OrdinalMapping
	secPkMap  val.OrdinalMapping
	prefixKD  val.TupleDesc
	prefixKB  *val.TupleBuilder
	secKb     *val.TupleBuilder
	batch     *skip.List
	secCur    *tree.Cursor
}

func newUniqAddValidator(ctx context.Context, finalSch schema.Schema, leftRows prolly.Map, ae *prolly.ArtifactsEditor, tm TableMerger) (*uniqAddValidator, error) {
	indexes := finalSch.Indexes().AllIndexes()
	primaryKD, _ := leftRows.Descriptors()
	primaryKB := val.NewTupleBuilder(primaryKD)

	var states []*validateIndexState
	for _, index := range indexes {
		if !index.IsUnique() || !tm.leftSch.Indexes().Contains(index.Name()) {
			continue
		}

		is, err := tm.leftTbl.GetIndexSet(ctx)
		idx, err := is.GetIndex(ctx, tm.leftSch, index.Name())
		if err != nil {
			return nil, err
		}
		m := durable.ProllyMapFromIndex(idx)
		if schema.IsKeyless(tm.leftSch) {
			m = prolly.ConvertToSecondaryKeylessIndex(m)
		}

		pkMapping := ordinalMappingFromIndex(index)
		meta, err := makeUniqViolMeta(finalSch, index)
		if err != nil {
			return nil, err
		}
		vInfo, err := json.Marshal(meta)
		if err != nil {
			return nil, err
		}

		kd := index.Schema().GetKeyDescriptor()
		prefixKD := kd.PrefixDesc(index.Count())
		prefixKB := val.NewTupleBuilder(prefixKD)

		secKb := val.NewTupleBuilder(m.KeyDesc())
		_, secPkMap := creation.GetIndexKeyMapping(tm.leftSch, index)

		states = append(states, &validateIndexState{
			index:     index,
			leftMap:   m,
			pkMapping: pkMapping,
			vInfo:     vInfo,
			prefixKB:  prefixKB,
			prefixKD:  prefixKD,
			secKb:     secKb,
			secPkMap:  secPkMap,
			batch: skip.NewSkipList(func(left, right []byte) int {
				return primaryKD.Compare(left, right)
			}),
		})
	}

	pkLen := tm.leftSch.GetPKCols().Size()
	if schema.IsKeyless(tm.leftSch) {
		pkLen = 1
	}

	return &uniqAddValidator{
		name:         tm.name,
		rightRootish: tm.rightSrc,
		ae:           ae,
		states:       states,
		leftRows:     leftRows,
		leftSch:      tm.leftSch,
		primaryKB:    primaryKB,
		primaryKD:    primaryKD,
		pkLen:        pkLen,
		batchSize:    uniqAddValidatorPendingSize,
	}, nil
}

// valid queues primary key changes for unique index validation. Primary keys
// are converted into secondaries for batching ordered lookups.
func (v *uniqAddValidator) valid(ctx context.Context, op tree.DiffOp, key, value val.Tuple) (int, error) {
	switch op {
	case tree.DiffOpDivergentModifyResolved, tree.DiffOpRightAdd, tree.DiffOpRightModify:
	default:
		return 0, fmt.Errorf("invalid unique validator diff type: %s", op)
	}
	var conflicts int
	for i, s := range v.states {
		secKey, foundNull := v.convertPriToSec(s, key, value)
		if foundNull {
			continue
		}

		s.batch.Put(secKey, value)
		if s.batch.Count() > v.batchSize {
			cnt, err := v.flush(ctx, i)
			conflicts += cnt
			if err != nil {
				return 0, err
			}
		}
	}
	return conflicts, nil
}

// convertPriToSec converts a key:value from the primary index into a
// secondary index key.
func (v *uniqAddValidator) convertPriToSec(s *validateIndexState, key, value val.Tuple) (val.Tuple, bool) {
	for to := range s.secPkMap {
		from := s.secPkMap.MapOrdinal(to)
		var field []byte
		if from < v.pkLen {
			field = key.GetField(from)
		} else {
			from -= v.pkLen
			field = value.GetField(from)
		}
		if field == nil {
			return nil, true
		}
		s.secKb.PutRaw(to, field)
	}
	return s.secKb.Build(s.leftMap.Pool()), false
}

// flush performs unique checks on a batch of sorted secondary keys.
func (v *uniqAddValidator) flush(ctx context.Context, i int) (int, error) {
	var conflicts int
	var err error

	s := v.states[i]
	iter := s.batch.IterAtStart()
	cur := s.secCur
	defer s.batch.Truncate()
	defer func() {
		s.secCur = nil
	}()

	var k, value []byte
	var key val.Tuple
	for {
		k, value = iter.Current()
		key = val.Tuple(k)
		if key == nil {
			break
		}
		iter.Advance()

		// pluck secondary prefix from secondary key (leading fields)
		for i := 0; i < s.prefixKD.Count(); i++ {
			s.prefixKB.PutRaw(i, key.GetField(i))
		}
		secKey := s.prefixKB.Build(v.leftRows.Pool())

		if cur == nil {
			s.secCur, err = tree.NewCursorAtKey(ctx, s.leftMap.NodeStore(), s.leftMap.Node(), val.Tuple(secKey), s.leftMap.KeyDesc())
			cur = s.secCur
		}

		err = tree.Seek(ctx, cur, secKey, s.prefixKD)
		if err != nil {
			return 0, err
		}
		if cur.Valid() {
			indexK := val.Tuple(cur.CurrentKey())
			if s.prefixKD.Compare(secKey, indexK) != 0 {
				continue
			}

			conflicts++

			// existingPk is the merge-left primary key that
			// generated the conflicting unique index key
			existingPK := getPKFromSecondaryKey(v.primaryKB, v.leftRows.Pool(), s.pkMapping, indexK)
			err = replaceUniqueKeyViolation(ctx, v.ae, v.leftRows, existingPK, v.primaryKD, v.rightRootish, s.vInfo, v.name)
			if err != nil {
				return 0, err
			}

			// newPk is the merge-right primary key whose secondary
			// index conflicts with existingPk
			newPK := getPKFromSecondaryKey(v.primaryKB, v.leftRows.Pool(), s.pkMapping, key)
			err = replaceUniqueKeyViolationWithValue(ctx, v.ae, newPK, value, v.primaryKD, v.rightRootish, s.vInfo, v.name)
			if err != nil {
				return 0, err
			}
		}
	}
	return conflicts, nil
}
func (v *uniqAddValidator) finalize(ctx context.Context) (int, error) {
	var conflicts int
	for i, _ := range v.states {
		cnt, err := v.flush(ctx, i)
		if err != nil {
			return 0, err
		}
		conflicts += cnt
	}
	return conflicts, nil
}

type diffMerger interface {
	merge(context.Context, tree.ThreeWayDiff) error
}

var _ diffMerger = (*conflictMerger)(nil)
var _ diffMerger = (*primaryMerger)(nil)
var _ diffMerger = (*secondaryMerger)(nil)

// conflictMerger processing primary key diffs
// with conflict types into artifact table writes.
type conflictMerger struct {
	ae           *prolly.ArtifactsEditor
	rightRootish hash.Hash
	meta         []byte
}

func newConflictMerger(ctx context.Context, tm TableMerger, ae *prolly.ArtifactsEditor) (*conflictMerger, error) {
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

// primaryMerger translaties three-way diffs
// on the primary index into merge-left updates.
type primaryMerger struct {
	serializer message.ProllyMapSerializer
	keyDesc    val.TupleDesc
	valDesc    val.TupleDesc
	ns         tree.NodeStore
	root       tree.Node
	cur        *tree.Cursor
	mut        *prolly.MutableMap
	key, value val.Tuple
}

func newPrimaryMerger(leftRows prolly.Map) (*primaryMerger, error) {
	return &primaryMerger{
		mut: leftRows.Mutate(),
	}, nil
}

func (m *primaryMerger) merge(ctx context.Context, diff tree.ThreeWayDiff) error {
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
	return m.mut.Put(ctx, newKey, newValue)
}

func (m *primaryMerger) finalize(ctx context.Context) (durable.Index, error) {
	mergedMap, err := m.mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	return durable.IndexFromProllyMap(mergedMap), nil
}

// secondaryMerger translates diffs on the primary index
// into secondary index updates.
type secondaryMerger struct {
	leftSet  durable.IndexSet
	rightSet durable.IndexSet
	leftMut  []MutableSecondaryIdx
}

const secondaryMergerPendingSize = 650_000

func newSecondaryMerger(ctx context.Context, tm TableMerger, finalSch schema.Schema) (*secondaryMerger, error) {
	ls, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	lm, err := GetMutableSecondaryIdxsWithPending(ctx, tm.leftSch, ls, secondaryMergerPendingSize)
	if err != nil {
		return nil, err
	}

	rs, err := tm.rightTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	return &secondaryMerger{
		leftSet:  ls,
		rightSet: rs,
		leftMut:  lm,
	}, nil
}

func (m *secondaryMerger) merge(ctx context.Context, diff tree.ThreeWayDiff) error {
	var err error
	for _, idx := range m.leftMut {
		switch diff.Op {
		case tree.DiffOpDivergentModifyResolved:
			err = applyEdit(ctx, idx, diff.Key, diff.Left, diff.Merged)
		case tree.DiffOpRightAdd, tree.DiffOpRightModify, tree.DiffOpRightDelete:
			err = applyEdit(ctx, idx, diff.Key, diff.Base, diff.Right)
		default:
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// finalize reifies edits into output index sets
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

// valueMerger attempts to resolve three-ways diffs on the same
// key but with conflicting values. A successful resolve produces
// a three-way cell edit (tree.DiffOpDivergentModifyResolved).
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
