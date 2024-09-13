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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/valutil"
	"github.com/dolthub/dolt/go/store/types"
)

type rowMergeResult struct {
	mergedRow    types.Value
	didCellMerge bool
	isConflict   bool
}

type rowMerger func(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (rowMergeResult, error)

type applicator func(ctx context.Context, sch schema.Schema, tableEditor editor.TableEditor, rowData types.Map, stats *MergeStats, change types.ValueChanged) error

// mergeNomsTable merges a Noms table, which includes updating row data, secondary index data, and applying the specified |mergedSch|.
func mergeNomsTable(ctx *sql.Context, tm *TableMerger, mergedSch schema.Schema, vrw types.ValueReadWriter, opts editor.Options) (*doltdb.Table, *MergeStats, error) {
	// For schema changes on Nom data, we don't need to migrate the existing data, because each column is mapped by
	// a hash key, and isn't a direct []byte lookup, like with Prolly storage, so it's safe to immediately update
	// the table to the merged schema.
	mergeTbl, err := tm.leftTbl.UpdateSchema(ctx, mergedSch)
	if err != nil {
		return nil, nil, err
	}

	// If any indexes were added during the merge, then we need to generate their row data to add to our updated table.
	addedIndexesSet := make(map[string]string)
	for _, index := range mergedSch.Indexes().AllIndexes() {
		addedIndexesSet[strings.ToLower(index.Name())] = index.Name()
	}
	for _, index := range tm.leftSch.Indexes().AllIndexes() {
		delete(addedIndexesSet, strings.ToLower(index.Name()))
	}
	for _, addedIndex := range addedIndexesSet {
		newIndexData, err := editor.RebuildIndex(ctx, mergeTbl, addedIndex, opts)
		if err != nil {
			return nil, nil, err
		}
		mergeTbl, err = mergeTbl.SetNomsIndexRows(ctx, addedIndex, newIndexData)
		if err != nil {
			return nil, nil, err
		}
	}

	updatedTblEditor, err := editor.NewTableEditor(ctx, mergeTbl, mergedSch, tm.name.Name, opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tm.leftTbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeRows, err := tm.rightTbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	ancRows, err := tm.ancTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	resultTbl, cons, stats, err := mergeNomsTableData(ctx, vrw, tm.name.Name, mergedSch, rows, mergeRows, durable.NomsMapFromIndex(ancRows), updatedTblEditor)
	if err != nil {
		return nil, nil, err
	}

	if cons.Len() > 0 {
		resultTbl, err = setConflicts(ctx, durable.ConflictIndexFromNomsMap(cons, vrw), tm.leftTbl, tm.rightTbl, tm.ancTbl, resultTbl)
		if err != nil {
			return nil, nil, err
		}
		stats.DataConflicts = int(cons.Len())
	}

	resultTbl, err = mergeAutoIncrementValues(ctx, tm.leftTbl, tm.rightTbl, resultTbl)
	if err != nil {
		return nil, nil, err
	}

	return resultTbl, stats, nil
}

func mergeNomsTableData(
	ctx *sql.Context,
	vrw types.ValueReadWriter,
	tblName string,
	sch schema.Schema,
	rows, mergeRows, ancRows types.Map,
	tblEdit editor.TableEditor,
) (*doltdb.Table, types.Map, *MergeStats, error) {
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

	originalCtx := ctx
	eg, errGrCtx := errgroup.WithContext(ctx)
	ctx = originalCtx.WithContext(errGrCtx)

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
					mkNilOrKeyLess, err = key.Less(ctx, vrw.Format(), mergeKey)
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
					keyNilOrMKLess, err = mergeKey.Less(ctx, vrw.Format(), key)
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

	mergedTable, err := tblEdit.Table(originalCtx)
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
			err = tableEditor.UpdateRow(ctx, oldRow, newRow, makeDupHandler(ctx, tableEditor, change.Key.(types.Tuple), change.NewValue.(types.Tuple)))
			if err != nil {
				if err != nil {
					return err
				}
			}
		} else {
			err = tableEditor.InsertRow(ctx, newRow, makeDupHandler(ctx, tableEditor, change.Key.(types.Tuple), change.NewValue.(types.Tuple)))
			if err != nil {
				if err != nil {
					return err
				}
			}
		}
		stats.Adds++
	case types.DiffChangeModified:
		key, oldVal, newVal := change.Key.(types.Tuple), change.OldValue.(types.Tuple), change.NewValue.(types.Tuple)
		oldRow, err := row.FromNoms(sch, key, oldVal)
		if err != nil {
			return err
		}
		newRow, err := row.FromNoms(sch, key, newVal)
		if err != nil {
			return err
		}
		err = tableEditor.UpdateRow(ctx, oldRow, newRow, makeDupHandler(ctx, tableEditor, key, newVal))
		if err != nil {
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

func makeDupHandler(ctx context.Context, tableEditor editor.TableEditor, newKey, newValue types.Tuple) editor.PKDuplicateCb {
	return func(keyString, indexName string, existingKey, existingValue types.Tuple, isPk bool) error {
		if isPk {
			return fmt.Errorf("duplicate key '%s'", keyString)
		}

		sch := tableEditor.Schema()
		idx := sch.Indexes().GetByName(indexName)
		m, err := makeUniqViolMeta(sch, idx)
		if err != nil {
			return err
		}
		d, err := json.Marshal(m)
		if err != nil {
			return err
		}

		err = addUniqueViolation(ctx, tableEditor, existingKey, existingValue, newKey, newValue, d)
		if err != nil {
			return err
		}

		return nil
	}
}

func addUniqueViolation(ctx context.Context, tableEditor editor.TableEditor, existingKey, existingVal, newKey, newVal types.Tuple, jsonData []byte) error {
	nomsJson, err := jsonDataToNomsValue(ctx, tableEditor.ValueReadWriter(), jsonData)
	if err != nil {
		return err
	}
	cvKey, cvVal, err := toConstraintViolationRow(ctx, CvType_UniqueIndex, nomsJson, newKey, newVal)
	if err != nil {
		return err
	}
	err = tableEditor.SetConstraintViolation(ctx, cvKey, cvVal)
	if err != nil {
		return err
	}

	cvKey, cvVal, err = toConstraintViolationRow(ctx, CvType_UniqueIndex, nomsJson, existingKey, existingVal)
	if err != nil {
		return err
	}
	err = tableEditor.SetConstraintViolation(ctx, cvKey, cvVal)
	if err != nil {
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
		} else {
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
