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
	uniq, err := newUniqValidator(ctx, finalSch, tm, ae)
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

		cnt, err := uniq.validateDiff(ctx, diff)
		if err != nil {
			return nil, nil, err
		}
		s.Conflicts += cnt

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

type uniqValidator struct {
	src     doltdb.Rootish
	srcHash hash.Hash
	edits   *prolly.ArtifactsEditor
	indexes []uniqIndex
}

func newUniqValidator(ctx context.Context, sch schema.Schema, tm TableMerger, edits *prolly.ArtifactsEditor) (uniqValidator, error) {
	srcHash, err := tm.rightSrc.HashOf()
	if err != nil {
		return uniqValidator{}, err
	}

	uv := uniqValidator{
		src:     tm.rightSrc,
		srcHash: srcHash,
		edits:   edits,
	}

	rows, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return uniqValidator{}, err
	}
	clustered := durable.ProllyMapFromIndex(rows)

	indexes, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return uniqValidator{}, err
	}

	for _, def := range sch.Indexes().AllIndexes() {
		if !def.IsUnique() {
			continue
		} else if !tm.leftSch.Indexes().Contains(def.Name()) {
			continue // todo: how do we validate in this case?
		}

		idx, err := indexes.GetIndex(ctx, sch, def.Name())
		if err != nil {
			return uniqValidator{}, err
		}
		secondary := durable.ProllyMapFromIndex(idx)

		u, err := newUniqIndex(sch, def, clustered, secondary)
		if err != nil {
			return uniqValidator{}, err
		}
		uv.indexes = append(uv.indexes, u)
	}
	return uv, nil
}

func (uv uniqValidator) validateDiff(ctx context.Context, diff tree.ThreeWayDiff) (conflicts int, err error) {
	var value val.Tuple
	switch diff.Op {
	case tree.DiffOpRightAdd, tree.DiffOpRightModify:
		value = diff.Right
	case tree.DiffOpDivergentModifyResolved:
		value = diff.Merged
	default:
		return
	}
	for _, idx := range uv.indexes {
		err = idx.findCollisions(ctx, diff.Key, value, func(k, v val.Tuple) error {
			return uv.insertArtifact(ctx, k, v, idx.meta)
		})
		if err != nil {
			break
		}
	}
	return
}

func (uv uniqValidator) insertArtifact(ctx context.Context, key, value val.Tuple, meta UniqCVMeta) error {
	vinfo, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	cvm := prolly.ConstraintViolationMeta{VInfo: vinfo, Value: value}
	return uv.edits.ReplaceConstraintViolation(ctx, key, uv.srcHash, prolly.ArtifactTypeUniqueKeyViol, cvm)
}

type uniqIndex struct {
	def       schema.Index
	secondary prolly.Map
	clustered prolly.Map
	meta      UniqCVMeta
	// map clustered key-value to
	// secondary index prefix keys
	prefixBld    *val.TupleBuilder
	prefixKeyMap val.OrdinalMapping
	prefixValMap val.OrdinalMapping
	// map secondary index keys to
	// clustered index keys
	clusteredBld *val.TupleBuilder
	clusteredMap val.OrdinalMapping
}

func newUniqIndex(sch schema.Schema, def schema.Index, clusterd, secondary prolly.Map) (uniqIndex, error) {
	meta, err := makeUniqViolMeta(sch, def)
	if err != nil {
		return uniqIndex{}, err
	}

	if schema.IsKeyless(sch) { // todo(andy): sad panda
		secondary = prolly.ConvertToSecondaryKeylessIndex(secondary)
	}

	// map clustered rows to secondary prefixes
	idxCols := schema.GetIndexedColumns(def)
	prefixBld := val.NewTupleBuilder(secondary.KeyDesc().PrefixDesc(idxCols.Size()))
	prefixKeyMap := schema.MakeColumnMapping(sch.GetPKCols(), idxCols)
	prefixValMap := schema.MakeColumnMapping(sch.GetNonPKCols(), idxCols)

	// map secondary index keys to clustered index keys
	// todo(andy): keyless schemas
	allCols := schema.GetAllColumns(def)
	clusteredBld := val.NewTupleBuilder(clusterd.KeyDesc())
	clusteredMap := schema.MakeColumnMapping(allCols, sch.GetPKCols())

	return uniqIndex{
		def:          def,
		secondary:    secondary,
		clustered:    clusterd,
		meta:         meta,
		prefixBld:    prefixBld,
		prefixKeyMap: prefixKeyMap,
		prefixValMap: prefixValMap,
		clusteredBld: clusteredBld,
		clusteredMap: clusteredMap,
	}, nil
}

type collisionFn func(key, value val.Tuple) error

func (idx uniqIndex) findCollisions(ctx context.Context, key, value val.Tuple, cb collisionFn) error {
	prefix := idx.secondaryPrefixFromRow(key, value)
	if keyHasNulls(prefix, idx.prefixBld.Desc) {
		return nil // NULLs cannot cause unique violations
	}

	var collision val.Tuple
	err := idx.secondary.GetPrefix(ctx, prefix, idx.prefixBld.Desc, func(k, _ val.Tuple) (err error) {
		collision = k
		return
	})
	if err != nil || collision == nil {
		return err
	}

	// |prefix| was non-unique, find the clustered index row that
	// collided with row(|key|, |value|) and pass both to |cb|
	pk := idx.clusteredKeyFromSecondaryKey(collision)
	err = idx.clustered.Get(ctx, pk, func(k val.Tuple, v val.Tuple) error {
		if k == nil {
			return fmt.Errorf("failed to find key: %s", idx.clustered.KeyDesc().Format(pk))
		}
		return cb(k, v)
	})
	if err != nil {
		return err
	}
	return cb(key, value)
}

func (idx uniqIndex) secondaryPrefixFromRow(key, value val.Tuple) val.Tuple {
	for to, from := range idx.prefixKeyMap {
		if from >= 0 {
			idx.prefixBld.PutRaw(to, key.GetField(from))
		}
	}
	for to, from := range idx.prefixValMap {
		if from >= 0 {
			idx.prefixBld.PutRaw(to, value.GetField(from))
		}
	}
	return idx.prefixBld.Build(idx.clustered.Pool())
}

func (idx uniqIndex) clusteredKeyFromSecondaryKey(key val.Tuple) val.Tuple {
	for to, from := range idx.clusteredMap {
		if from >= 0 {
			idx.clusteredBld.PutRaw(to, key.GetField(from))
		}
	}
	return idx.clusteredBld.Build(idx.clustered.Pool())
}

func keyHasNulls(key val.Tuple, desc val.TupleDesc) bool {
	for i := range desc.Types {
		if desc.GetField(i, key) == nil {
			return true
		}
	}
	return false
}

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
