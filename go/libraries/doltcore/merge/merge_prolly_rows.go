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

// mergeProllyTable merges the table specified by |tm| using the specified |mergedSch| and returns the new table
// instance, along with merge stats and any error. This function will merge the table artifacts (e.g. recorded
// conflicts), migrate any existing table data to the specified |mergedSch|, and merge table data from both sides
// of the merge together.
func mergeProllyTable(ctx context.Context, tm *TableMerger, mergedSch schema.Schema) (*doltdb.Table, *MergeStats, error) {
	err := maybeAbortDueToUnmergeableIndexes(tm.name, tm.leftSch, tm.rightSch, mergedSch)
	if err != nil {
		return nil, nil, err
	}

	mergeTbl, err := mergeTableArtifacts(ctx, tm, tm.leftTbl)
	if err != nil {
		return nil, nil, err
	}
	tm.leftTbl = mergeTbl

	// Before we merge the table data we need to fix up the primary index on the left-side of the merge for
	// any ordinal mapping changes (i.e. moving/dropping/adding columns).
	// NOTE: This won't ALWAYS be the left side... eventually we will need to optimize which side we pick
	//       (i.e. the side that needs the least work to modify) and make this logic work for either side.
	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	leftRows := durable.ProllyMapFromIndex(lr)
	valueMerger := newValueMerger(mergedSch, tm.leftSch, tm.rightSch, tm.ancSch, leftRows.Pool())
	leftMapping := valueMerger.leftMapping

	// Migrate primary index data to rewrite the values on the left side of the merge if necessary
	schemasDifferentSize := len(tm.leftSch.GetAllCols().GetColumns()) != len(mergedSch.GetAllCols().GetColumns())
	if schemasDifferentSize || leftMapping.IsIdentityMapping() == false {
		if err := migrateDataToMergedSchema(ctx, tm, valueMerger, mergedSch); err != nil {
			return nil, nil, err
		}

		// After we migrate the data on the left-side to the new, merged schema, we reset
		// the left mapping to an identity mapping, since it's a direct mapping now.
		valueMerger.leftMapping = val.NewIdentityOrdinalMapping(len(valueMerger.leftMapping))
	}

	// After we've migrated the existing data to the new schema, it's safe for us to update the schema on the table
	mergeTbl, err = tm.leftTbl.UpdateSchema(ctx, mergedSch)
	if err != nil {
		return nil, nil, err
	}

	var stats *MergeStats
	mergeTbl, stats, err = mergeProllyTableData(ctx, tm, mergedSch, mergeTbl, valueMerger)
	if err != nil {
		return nil, nil, err
	}

	n, err := mergeTbl.NumRowsInConflict(ctx)
	if err != nil {
		return nil, nil, err
	}
	stats.Conflicts = int(n)

	mergeTbl, err = mergeAutoIncrementValues(ctx, tm.leftTbl, tm.rightTbl, mergeTbl)
	if err != nil {
		return nil, nil, err
	}
	return mergeTbl, stats, nil
}

// mergeProllyTableData three-way merges the data for a given table. We currently take the left
// side of the merge and use that data as the starting point to merge in changes from the right
// side. Eventually, we will need to optimize this to pick the side that needs the least work.
// We iterate over the calculated diffs using a ThreeWayDiffer instance, and for every change
// to the right-side, we apply it to the left-side by merging it into the left-side's primary index
// as well as any secondary indexes, and also checking for unique constraints incrementally. When
// conflicts are detected, this function attempts to resolve them automatically if possible, and
// if not, they are recorded as conflicts in the table's artifacts.
func mergeProllyTableData(ctx context.Context, tm *TableMerger, finalSch schema.Schema, mergeTbl *doltdb.Table, valueMerger *valueMerger) (*doltdb.Table, *MergeStats, error) {
	iter, err := threeWayDiffer(ctx, tm, valueMerger)
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

	pri, err := newPrimaryMerger(leftRows, valueMerger, finalSch)
	if err != nil {
		return nil, nil, err
	}
	sec, err := newSecondaryMerger(ctx, tm, valueMerger, finalSch)
	if err != nil {
		return nil, nil, err
	}
	conflicts, err := newConflictMerger(ctx, tm, ae)
	if err != nil {
		return nil, nil, err
	}
	// validator shares editor with conflict merge
	uniq, err := newUniqAddValidator(ctx, finalSch, leftRows, ae, tm, valueMerger)
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
			// In this case, a modification or delete was made to one side, and a conflicting delete or modification
			// was made to the other side, so these cannot be automatically resolved.
			s.Conflicts++
			err = conflicts.merge(ctx, diff, nil)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightAdd:
			s.Adds++
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.rightSch)
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
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.rightSch)
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
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpDivergentModifyResolved:
			// In this case, both sides of the merge have made different changes to a row, but we were able to
			// resolve them automatically.
			s.Modifications++
			err = pri.merge(ctx, diff, nil)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, nil)
			if err != nil {
				return nil, nil, err
			}
			cnt, err := uniq.valid(ctx, diff.Op, diff.Key, diff.Merged)
			if err != nil {
				return nil, nil, err
			}
			s.Conflicts += cnt
		case tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify, tree.DiffOpConvergentDelete:
			// In this case, both sides of the merge have made the same change, so no additional changes are needed.
			if keyless {
				s.Conflicts++
				err = conflicts.merge(ctx, diff, nil)
				if err != nil {
					return nil, nil, err
				}
			}
		default:
			// Currently, all changes are applied to the left-side of the merge, so for any left-side diff ops,
			// we can simply ignore them since that data is already in the destination (the left-side).
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

func maybeAbortDueToUnmergeableIndexes(tableName string, leftSchema, rightSchema, targetSchema schema.Schema) error {
	leftOk, err := validateTupleFields(leftSchema, targetSchema)
	if err != nil {
		return err
	}

	rightOk, err := validateTupleFields(rightSchema, targetSchema)
	if err != nil {
		return err
	}

	if !leftOk || !rightOk {
		return fmt.Errorf("table %s can't be automatically merged.\nTo merge this table, make the schema on the source and target branch equal.", tableName)
	}

	return nil
}

func threeWayDiffer(ctx context.Context, tm *TableMerger, valueMerger *valueMerger) (*tree.ThreeWayDiffer[val.Tuple, val.TupleDesc], error) {
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

	return tree.NewThreeWayDiffer(ctx, leftRows.NodeStore(), leftRows.Tuples(), rightRows.Tuples(), ancRows.Tuples(), valueMerger.tryMerge, valueMerger.keyless, leftRows.Tuples().Order)
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
	valueMerger  *valueMerger
	tm           *TableMerger
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

func newUniqAddValidator(ctx context.Context, finalSch schema.Schema, leftRows prolly.Map, ae *prolly.ArtifactsEditor, tm *TableMerger, valueMerger *valueMerger) (*uniqAddValidator, error) {
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
		_, secPkMap := creation.GetIndexKeyMapping(tm.rightSch, index)

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
		valueMerger:  valueMerger,
		tm:           tm,
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
	for to, from := range s.secPkMap {
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
			// Don't map the value to the merged schema if the table is keyless (since they
			// don't allow schema changes) or if the mapping is an identity mapping.
			if !v.valueMerger.keyless && !v.valueMerger.rightMapping.IsIdentityMapping() {
				modifiedValue := remapTuple(value, v.tm.rightSch.GetValueDescriptor(), v.valueMerger.rightMapping)
				value = val.NewTuple(v.valueMerger.syncPool, modifiedValue...)
			}

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

// conflictMerger processing primary key diffs
// with conflict types into artifact table writes.
type conflictMerger struct {
	ae           *prolly.ArtifactsEditor
	rightRootish hash.Hash
	meta         []byte
}

func newConflictMerger(ctx context.Context, tm *TableMerger, ae *prolly.ArtifactsEditor) (*conflictMerger, error) {
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

// TODO: Do we actually need the schema.Schema var?
func (m *conflictMerger) merge(ctx context.Context, diff tree.ThreeWayDiff, _ schema.Schema) error {
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

// primaryMerger translates three-way diffs
// on the primary index into merge-left updates.
type primaryMerger struct {
	serializer  message.ProllyMapSerializer
	keyDesc     val.TupleDesc
	valDesc     val.TupleDesc
	ns          tree.NodeStore
	root        tree.Node
	cur         *tree.Cursor
	mut         *prolly.MutableMap
	key, value  val.Tuple
	valueMerger *valueMerger
	finalSch    schema.Schema
}

func newPrimaryMerger(leftRows prolly.Map, valueMerger *valueMerger, finalSch schema.Schema) (*primaryMerger, error) {
	return &primaryMerger{
		mut:         leftRows.Mutate(),
		valueMerger: valueMerger,
		finalSch:    finalSch,
	}, nil
}

// merge applies the specified |diff| to the primary index of this primaryMerger. The given |sourceSch|
// specifies the schema of the source of the diff, which is used to map the diff to the post-merge
// schema. |sourceSch| may be nil when no mapping from the source schema is needed (i.e. DiffOpRightDelete,
// and DiffOpDivergentModifyResolved).
func (m *primaryMerger) merge(ctx context.Context, diff tree.ThreeWayDiff, sourceSch schema.Schema) error {
	switch diff.Op {
	case tree.DiffOpRightAdd, tree.DiffOpRightModify:
		if sourceSch == nil {
			return fmt.Errorf("no source schema specified to map right-side changes to merged schema")
		}

		newTupleValue := diff.Right
		if schema.IsKeyless(sourceSch) {
			if m.valueMerger.rightMapping.IsIdentityMapping() == false {
				return fmt.Errorf("cannot merge keyless tables with reordered columns")
			}
		} else {
			modifiedValue := remapTuple(diff.Right, sourceSch.GetValueDescriptor(), m.valueMerger.rightMapping)
			newTupleValue = val.NewTuple(m.valueMerger.syncPool, modifiedValue...)
		}
		return m.mut.Put(ctx, diff.Key, newTupleValue)
	case tree.DiffOpRightDelete:
		return m.mut.Put(ctx, diff.Key, diff.Right)
	case tree.DiffOpDivergentModifyResolved:
		// TODO: This data should have already been mapped for any schema changes by tryMerge, but we
		//       don't have test coverage yet, so need to add something to trigger this code path.
		return m.mut.Put(ctx, diff.Key, diff.Merged)
	default:
		return fmt.Errorf("unexpected diffOp for editing primary index: %s", diff.Op)
	}
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
	leftSet      durable.IndexSet
	rightSet     durable.IndexSet
	leftMut      []MutableSecondaryIdx
	valueMerger  *valueMerger
	mergedSchema schema.Schema
}

const secondaryMergerPendingSize = 650_000

func newSecondaryMerger(ctx context.Context, tm *TableMerger, valueMerger *valueMerger, mergedSchema schema.Schema) (*secondaryMerger, error) {
	ls, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	// Use the mergedSchema to work with the secondary indexes, to pull out row data using the right
	// pri_index -> sec_index mapping.
	lm, err := GetMutableSecondaryIdxsWithPending(ctx, mergedSchema, ls, secondaryMergerPendingSize)
	if err != nil {
		return nil, err
	}

	rs, err := tm.rightTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	return &secondaryMerger{
		leftSet:      ls,
		rightSet:     rs,
		leftMut:      lm,
		valueMerger:  valueMerger,
		mergedSchema: mergedSchema,
	}, nil
}

func (m *secondaryMerger) merge(ctx context.Context, diff tree.ThreeWayDiff, sourceSch schema.Schema) error {
	var err error
	for _, idx := range m.leftMut {
		switch diff.Op {
		case tree.DiffOpDivergentModifyResolved:
			err = applyEdit(ctx, idx, diff.Key, diff.Left, diff.Merged)
		case tree.DiffOpRightAdd, tree.DiffOpRightModify:
			// Just as with the primary index, we need to map right-side changes to the final, merged schema.
			if sourceSch == nil {
				return fmt.Errorf("no source schema specified to map right-side changes to merged schema")
			}

			newTupleValue := diff.Right
			if schema.IsKeyless(sourceSch) {
				if m.valueMerger.rightMapping.IsIdentityMapping() == false {
					return fmt.Errorf("cannot merge keyless tables with reordered columns")
				}
			} else {
				valueMappedToMergeSchema := remapTuple(diff.Right, sourceSch.GetValueDescriptor(), m.valueMerger.rightMapping)
				newTupleValue = val.NewTuple(m.valueMerger.syncPool, valueMappedToMergeSchema...)
			}

			err = applyEdit(ctx, idx, diff.Key, diff.Base, newTupleValue)
		case tree.DiffOpRightDelete:
			err = applyEdit(ctx, idx, diff.Key, diff.Base, diff.Right)
		default:
			// Any changes to the left-side of the merge are not needed, since we currently
			// always default to using the left side of the merge as the final result, so all
			// left-side changes are already there. This won't always be the case though! We'll
			// eventually want to optimize the merge side we choose for applying changes and
			// will need to update this code.
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

// remapTuple takes the given |tuple| and the |desc| that describes its data, and uses |mapping| to map the tuple's
// data into a new [][]byte, as indicated by the specified ordinal mapping.
func remapTuple(tuple val.Tuple, desc val.TupleDesc, mapping val.OrdinalMapping) [][]byte {
	result := make([][]byte, len(mapping))
	for to, from := range mapping {
		if from == -1 {
			continue
		}
		result[to] = desc.GetField(from, tuple)
	}

	return result
}

func mergeTableArtifacts(ctx context.Context, tm *TableMerger, mergeTbl *doltdb.Table) (*doltdb.Table, error) {
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
	leftMapping, rightMapping, baseMapping := generateSchemaMappings(merged, leftSch, rightSch, baseSch)

	return &valueMerger{
		numCols:      merged.GetNonPKCols().Size(),
		vD:           merged.GetValueDescriptor(),
		leftMapping:  leftMapping,
		rightMapping: rightMapping,
		baseMapping:  baseMapping,
		syncPool:     syncPool,
		keyless:      schema.IsKeyless(merged),
	}
}

// generateSchemaMappings returns three schema mappings: 1) mapping the |leftSch| to |mergedSch|,
// 2) mapping |rightSch| to |mergedSch|, and 3) mapping |baseSch| to |mergedSch|. Columns are
// mapped from the source schema to destination schema by finding an identical tag, or if no
// identical tag is found, then falling back to a match on column name and type.
func generateSchemaMappings(mergedSch, leftSch, rightSch, baseSch schema.Schema) (leftMapping, rightMapping, baseMapping val.OrdinalMapping) {
	n := mergedSch.GetNonPKCols().Size()
	leftMapping = make(val.OrdinalMapping, n)
	rightMapping = make(val.OrdinalMapping, n)
	baseMapping = make(val.OrdinalMapping, n)

	for i, col := range mergedSch.GetNonPKCols().GetColumns() {
		leftMapping[i] = findNonPKColumnMappingByTagOrName(leftSch, col)
		rightMapping[i] = findNonPKColumnMappingByTagOrName(rightSch, col)
		baseMapping[i] = findNonPKColumnMappingByTagOrName(baseSch, col)
	}

	return leftMapping, rightMapping, baseMapping
}

// findNonPKColumnMappingByName returns the index of the column with the given name in the given schema, or -1 if it
// doesn't exist.
func findNonPKColumnMappingByName(sch schema.Schema, name string) int {
	leftNonPKCols := sch.GetNonPKCols()
	if leftNonPKCols.Contains(name) {
		return leftNonPKCols.IndexOf(name)
	} else {
		return -1
	}
}

// findNonPKColumnMappingByTagOrName returns the index of the column with the given tag in the given schema. If a
// matching tag is not found, then this function falls back to looking for a matching column by name. If no
// matching column is found, then this function returns -1.
func findNonPKColumnMappingByTagOrName(sch schema.Schema, col schema.Column) int {
	if idx, ok := sch.GetNonPKCols().TagToIdx[col.Tag]; ok {
		return idx
	} else {
		return findNonPKColumnMappingByName(sch, col.Name)
	}
}

// migrateDataToMergedSchema migrates the data from the left side of the merge of a table to the merged schema. This
// currently only includes updating the primary index. This is necessary when a schema change is
// being applied, so that when the new schema is used to pull out data from the table, it will be in the right order.
func migrateDataToMergedSchema(ctx context.Context, tm *TableMerger, vm *valueMerger, mergedSch schema.Schema) error {
	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return err
	}
	leftRows := durable.ProllyMapFromIndex(lr)
	mut := leftRows.Mutate()
	mapIter, err := mut.IterAll(ctx)
	if err != nil {
		return err
	}

	leftSch, err := tm.leftTbl.GetSchema(ctx)
	if err != nil {
		return err
	}
	valueDescriptor := leftSch.GetValueDescriptor()

	for {
		key, value, err := mapIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		pool := vm.syncPool
		modifiedValue := remapTuple(value, valueDescriptor, vm.leftMapping)
		modifiedValueAsTuple := val.NewTuple(pool, modifiedValue...)
		err = mut.Put(ctx, key, modifiedValueAsTuple)
		if err != nil {
			return err
		}
	}

	m, err := mut.Map(ctx)
	if err != nil {
		return err
	}

	newIndex := durable.IndexFromProllyMap(m)
	newTable, err := tm.leftTbl.UpdateRows(ctx, newIndex)
	if err != nil {
		return err
	}
	tm.leftTbl = newTable

	// TODO: for now... we don't actually need to migrate any of the data held in secondary indexes (yet).
	//       We're currently dealing with column adds/drops/renames/reorders, but none of those directly affect
	//       secondary indexes. Columns drops *should*, but currently Dolt just drops any index referencing the
	//       dropped column, so there's nothing to do currently.
	//       https://github.com/dolthub/dolt/issues/5641
	//       Once we start handling type changes changes or primary key changes, or fix the bug above,
	//       then we will need to start migrating secondary index data, too.

	return nil
}

// tryMerge performs a cell-wise merge given left, right, and base cell value
// tuples. It returns the merged cell value tuple and a bool indicating if a
// conflict occurred. tryMerge should only be called if left and right produce
// non-identical diffs against base.
func (m *valueMerger) tryMerge(left, right, base val.Tuple) (val.Tuple, bool) {
	// We can't merge two divergent keyless rows
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
