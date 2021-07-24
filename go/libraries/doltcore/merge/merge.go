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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	json2 "github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/valutil"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrFastForward = errors.New("fast forward")
var ErrSameTblAddedTwice = errors.New("table with same name added in 2 commits can't be merged")

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

// MergeTable merges schema and table data for the table tblName.
func (merger *Merger) MergeTable(ctx context.Context, tblName string, sess *editor.TableEditSession) (*doltdb.Table, *MergeStats, error) {
	tbl, ok, err := merger.root.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, err
	}

	var h hash.Hash
	var tblSchema schema.Schema
	if ok {
		h, err = tbl.HashOf()
		if err != nil {
			return nil, nil, err
		}
		tblSchema, err = tbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	mergeTbl, mergeOk, err := merger.mergeRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, err
	}

	var mh hash.Hash
	var mergeTblSchema schema.Schema
	if mergeOk {
		mh, err = mergeTbl.HashOf()
		if err != nil {
			return nil, nil, err
		}
		mergeTblSchema, err = mergeTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	ancTbl, ancOk, err := merger.ancRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, err
	}

	var anch hash.Hash
	var ancTblSchema schema.Schema
	var ancRows types.Map
	if ancOk {
		anch, err = ancTbl.HashOf()
		if err != nil {
			return nil, nil, err
		}
		ancTblSchema, err = ancTbl.GetSchema(ctx)
		if err != nil {
			return nil, nil, err
		}
		ancRows, err = ancTbl.GetRowData(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	{ // short-circuit logic
		if ancOk && schema.IsKeyless(ancTblSchema) {
			if ok && mergeOk && ancOk && h == mh && h == anch {
				return tbl, &MergeStats{Operation: TableUnmodified}, nil
			}
		} else {
			if ok && mergeOk && h == mh {
				return tbl, &MergeStats{Operation: TableUnmodified}, nil
			}
		}

		if !ancOk {
			if mergeOk && ok {
				if schema.SchemasAreEqual(tblSchema, mergeTblSchema) {
					// hackity hack
					ancTblSchema, ancTbl = tblSchema, tbl
					ancRows, _ = types.NewMap(ctx, merger.vrw)
				} else {
					return nil, nil, ErrSameTblAddedTwice
				}
			} else if ok {
				// fast-forward
				return tbl, &MergeStats{Operation: TableUnmodified}, nil
			} else {
				// fast-forward
				return mergeTbl, &MergeStats{Operation: TableAdded}, nil
			}
		}

		if h == anch {
			// fast-forward
			ms := MergeStats{Operation: TableModified}
			if h != mh {
				ms, err = calcTableMergeStats(ctx, tbl, mergeTbl)
				if err != nil {
					return nil, nil, err
				}
			}
			// force load the table editor since this counts as a change
			_, err := sess.GetTableEditor(ctx, tblName, nil)
			if err != nil {
				return nil, nil, err
			}
			return mergeTbl, &ms, nil
		} else if mh == anch {
			// fast-forward
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		}
	}

	postMergeSchema, schConflicts, err := SchemaMerge(tblSchema, mergeTblSchema, ancTblSchema, tblName)
	if err != nil {
		return nil, nil, err
	}
	if schConflicts.Count() != 0 {
		// error on schema conflicts for now
		return nil, nil, schConflicts.AsError()
	}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeRows, err := mergeTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	updatedTbl, err := tbl.UpdateSchema(ctx, postMergeSchema)
	if err != nil {
		return nil, nil, err
	}

	// If any indexes were added during the merge, then we need to generate their row data to add to our updated table.
	addedIndexesSet := make(map[string]string)
	for _, index := range postMergeSchema.Indexes().AllIndexes() {
		addedIndexesSet[strings.ToLower(index.Name())] = index.Name()
	}
	for _, index := range tblSchema.Indexes().AllIndexes() {
		delete(addedIndexesSet, strings.ToLower(index.Name()))
	}
	for _, addedIndex := range addedIndexesSet {
		newIndexData, err := editor.RebuildIndex(ctx, updatedTbl, addedIndex)
		if err != nil {
			return nil, nil, err
		}
		updatedTbl, err = updatedTbl.SetIndexRowData(ctx, addedIndex, newIndexData)
		if err != nil {
			return nil, nil, err
		}
	}

	err = sess.UpdateRoot(ctx, func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
		return root.PutTable(ctx, tblName, updatedTbl)
	})
	if err != nil {
		return nil, nil, err
	}

	updatedTblEditor, err := sess.GetTableEditor(ctx, tblName, nil)
	if err != nil {
		return nil, nil, err
	}

	resultTbl, conflicts, stats, err := mergeTableData(ctx, merger.vrw, tblName, postMergeSchema, rows, mergeRows, ancRows, updatedTblEditor, sess)
	if err != nil {
		return nil, nil, err
	}

	if conflicts.Len() > 0 {

		asr, err := ancTbl.GetSchemaRef()
		if err != nil {
			return nil, nil, err
		}

		sr, err := tbl.GetSchemaRef()
		if err != nil {
			return nil, nil, err
		}

		msr, err := mergeTbl.GetSchemaRef()
		if err != nil {
			return nil, nil, err
		}

		schemas := doltdb.NewConflict(asr, sr, msr)
		resultTbl, err = resultTbl.SetConflicts(ctx, schemas, conflicts)
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

func calcTableMergeStats(ctx context.Context, tbl *doltdb.Table, mergeTbl *doltdb.Table) (MergeStats, error) {
	rows, err := tbl.GetRowData(ctx)

	if err != nil {
		return MergeStats{}, err
	}

	mergeRows, err := mergeTbl.GetRowData(ctx)

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

	ms := MergeStats{Operation: TableModified}
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

type rowMerger func(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (types.Value, bool, error)

type applicator func(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, rowData types.Map, stats *MergeStats, change types.ValueChanged) error

func mergeTableData(ctx context.Context, vrw types.ValueReadWriter, tblName string, sch schema.Schema, rows, mergeRows, ancRows types.Map, tblEdit editor.TableEditor, sess *editor.TableEditSession) (*doltdb.Table, types.Map, *MergeStats, error) {
	var rowMerge rowMerger
	var applyChange applicator
	if schema.IsKeyless(sch) {
		rowMerge = keylessRowMerge
		applyChange = applyKeylessChange
	} else {
		rowMerge = pkRowMerge
		applyChange = applyPkChange
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
				mergedRow, isConflict, err := rowMerge(ctx, vrw.Format(), sch, r, mergeRow, ancRow)
				if err != nil {
					return err
				}

				if isConflict {
					stats.Conflicts++
					conflictTuple, err := doltdb.NewConflict(ancRow, r, mergeRow).ToNomsList(vrw)
					if err != nil {
						return err
					}

					err = addConflict(conflictValChan, sm.Done(), key, conflictTuple)
					if err != nil {
						return err
					}
				} else {
					vc := types.ValueChanged{ChangeType: change.ChangeType, Key: key, OldValue: ancRow, NewValue: mergedRow}
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

	if err := eg.Wait(); err != nil {
		return nil, types.EmptyMap, nil, err
	}

	conflicts, err := sm.Wait()
	if err != nil {
		return nil, types.EmptyMap, nil, err
	}
	newRoot, err := sess.Flush(ctx)
	if err != nil {
		return nil, types.EmptyMap, nil, err
	}

	mergedTable, ok, err := newRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, types.EmptyMap, nil, err
	}
	if !ok {
		return nil, types.EmptyMap, nil, fmt.Errorf("updated mergedTable `%s` has disappeared", tblName)
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

func applyPkChange(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, rowData types.Map, stats *MergeStats, change types.ValueChanged) error {
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

// applyPkChangeUnqErr handles unique key errors for the applyPkChange if an error is returned from a table editor.
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

func pkRowMerge(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (types.Value, bool, error) {
	var baseVals row.TaggedValues
	if baseRow == nil {
		if r.Equals(mergeRow) {
			// same row added to both
			return r, false, nil
		}
	} else if r == nil && mergeRow == nil {
		// same row removed from both
		return nil, false, nil
	} else if r == nil || mergeRow == nil {
		// removed from one and modified in another
		return nil, true, nil
	} else {
		var err error
		baseVals, err = row.ParseTaggedValues(baseRow.(types.Tuple))

		if err != nil {
			return nil, false, err
		}
	}

	rowVals, err := row.ParseTaggedValues(r.(types.Tuple))
	if err != nil {
		return nil, false, err
	}

	mergeVals, err := row.ParseTaggedValues(mergeRow.(types.Tuple))
	if err != nil {
		return nil, false, err
	}

	processTagFunc := func(tag uint64) (resultVal types.Value, isConflict bool) {
		baseVal, _ := baseVals.Get(tag)
		val, _ := rowVals.Get(tag)
		mergeVal, _ := mergeVals.Get(tag)

		if valutil.NilSafeEqCheck(val, mergeVal) {
			return val, false
		} else {
			modified := !valutil.NilSafeEqCheck(val, baseVal)
			mergeModified := !valutil.NilSafeEqCheck(mergeVal, baseVal)
			switch {
			case modified && mergeModified:
				return nil, true
			case modified:
				return val, false
			default:
				return mergeVal, false
			}
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
		return nil, false, err
	}

	if isConflict {
		return nil, true, nil
	}

	tpl := resultVals.NomsTupleForNonPKCols(nbf, sch.GetNonPKCols())
	v, err := tpl.Value(ctx)

	if err != nil {
		return nil, false, err
	}

	return v, false, nil
}

func keylessRowMerge(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, val, mergeVal, ancVal types.Value) (types.Value, bool, error) {
	// both sides of the merge produced a diff for this key,
	// so we always throw a conflict
	return nil, true, nil
}

func mergeAutoIncrementValues(ctx context.Context, tbl, otherTbl, resultTbl *doltdb.Table) (*doltdb.Table, error) {
	// only need to check one table, no PK changes yet
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	auto := false
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			auto, stop = true, true
		}
		return
	})
	if !auto {
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
	less, err := autoVal.Less(tbl.Format(), mergeAutoVal)
	if err != nil {
		return nil, err
	}
	if less {
		autoVal = mergeAutoVal
	}
	return resultTbl.SetAutoIncrementValue(autoVal)
}

func MergeCommits(ctx context.Context, commit, mergeCommit *doltdb.Commit) (*doltdb.RootValue, map[string]*MergeStats, error) {
	ancCommit, err := doltdb.GetCommitAncestor(ctx, commit, mergeCommit)
	if err != nil {
		return nil, nil, err
	}

	ourRoot, err := commit.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	theirRoot, err := mergeCommit.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	ancRoot, err := ancCommit.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	return MergeRoots(ctx, ourRoot, theirRoot, ancRoot)
}

func MergeRoots(ctx context.Context, ourRoot, theirRoot, ancRoot *doltdb.RootValue) (*doltdb.RootValue, map[string]*MergeStats, error) {
	merger := NewMerger(ctx, ourRoot, theirRoot, ancRoot, ourRoot.VRW())

	tblNames, err := doltdb.UnionTableNames(ctx, ourRoot, theirRoot)

	if err != nil {
		return nil, nil, err
	}

	tblToStats := make(map[string]*MergeStats)

	newRoot := ourRoot
	tableEditSession := editor.CreateTableEditSession(ourRoot, editor.TableEditSessionProps{
		ForeignKeyChecksDisabled: true,
	})
	// need to validate merges can be done on all tables before starting the actual merges.
	for _, tblName := range tblNames {
		mergedTable, stats, err := merger.MergeTable(ctx, tblName, tableEditSession)

		if err != nil {
			return nil, nil, err
		}

		if mergedTable != nil {
			tblToStats[tblName] = stats

			err = tableEditSession.UpdateRoot(ctx, func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
				return root.PutTable(ctx, tblName, mergedTable)
			})
			if err != nil {
				return nil, nil, err
			}
			newRoot, err = tableEditSession.Flush(ctx)
			if err != nil {
				return nil, nil, err
			}
		} else if has, err := newRoot.HasTable(ctx, tblName); err != nil {
			return nil, nil, err
		} else if has {
			tblToStats[tblName] = &MergeStats{Operation: TableRemoved}
			err = tableEditSession.UpdateRoot(ctx, func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
				return root.RemoveTables(ctx, tblName)
			})
			if err != nil {
				return nil, nil, err
			}
			newRoot, err = tableEditSession.Flush(ctx)
			if err != nil {
				return nil, nil, err
			}
		} else {
			panic("?")
		}
	}

	err = tableEditSession.UpdateRoot(ctx, func(ctx context.Context, root *doltdb.RootValue) (value *doltdb.RootValue, err error) {
		mergedFKColl, conflicts, err := ForeignKeysMerge(ctx, root, ourRoot, theirRoot, ancRoot)
		if err != nil {
			return nil, err
		}
		if len(conflicts) > 0 {
			return nil, fmt.Errorf("foreign key conflicts")
		}

		root, err = root.PutForeignKeyCollection(ctx, mergedFKColl)
		if err != nil {
			return nil, err
		}

		return root.UpdateSuperSchemasFromOther(ctx, tblNames, theirRoot)
	})
	if err != nil {
		return nil, nil, err
	}

	newRoot, err = tableEditSession.Flush(ctx)
	if err != nil {
		return nil, nil, err
	}

	newRoot, err = AddConstraintViolations(ctx, newRoot, ancRoot)
	if err != nil {
		return nil, nil, err
	}
	for tblName, stats := range tblToStats {
		tbl, ok, err := newRoot.GetTable(ctx, tblName)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			cvMap, err := tbl.GetConstraintViolations(ctx)
			if err != nil {
				return nil, nil, err
			}
			stats.ConstraintViolations = int(cvMap.Len())
		}
	}

	return newRoot, tblToStats, nil
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

func GetDocsInConflict(ctx context.Context, workingRoot *doltdb.RootValue, drw env.DocsReadWriter) (*diff.DocDiffs, error) {
	docs, err := drw.GetDocsOnDisk()
	if err != nil {
		return nil, err
	}

	return diff.NewDocDiffs(ctx, workingRoot, nil, docs)
}
