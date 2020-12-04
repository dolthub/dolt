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
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
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
func (merger *Merger) MergeTable(ctx context.Context, tblName string, tableEditSession *doltdb.TableEditSession) (*doltdb.Table, *MergeStats, error) {
	tbl, ok, err := merger.root.GetTable(ctx, tblName)

	if err != nil {
		return nil, nil, err
	}

	var h hash.Hash
	if ok {
		h, err = tbl.HashOf()

		if err != nil {
			return nil, nil, err
		}
	}

	mergeTbl, mergeOk, err := merger.mergeRoot.GetTable(ctx, tblName)

	if err != nil {
		return nil, nil, err
	}

	var mh hash.Hash
	if mergeOk {
		mh, err = mergeTbl.HashOf()

		if err != nil {
			return nil, nil, err
		}
	}

	ancTbl, ancOk, err := merger.ancRoot.GetTable(ctx, tblName)

	if err != nil {
		return nil, nil, err
	}

	var anch hash.Hash
	if ancOk {
		anch, err = ancTbl.HashOf()

		if err != nil {
			return nil, nil, err
		}
	}

	if ok && mergeOk && h == mh {
		return tbl, &MergeStats{Operation: TableUnmodified}, nil
	}

	if !ancOk {
		if mergeOk && ok {
			return nil, nil, ErrSameTblAddedTwice
		} else if ok {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		} else {
			return mergeTbl, &MergeStats{Operation: TableAdded}, nil
		}
	}

	if h == anch {
		ms := MergeStats{Operation: TableModified}
		if h != mh {
			ms, err = calcTableMergeStats(ctx, tbl, mergeTbl)
		}
		// force load the table editor since this counts as a change
		_, err := tableEditSession.GetTableEditor(ctx, tblName, nil)
		if err != nil {
			return nil, nil, err
		}
		return mergeTbl, &ms, nil
	} else if mh == anch {
		return tbl, &MergeStats{Operation: TableUnmodified}, nil
	}

	tblSchema, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, nil, err
	}

	mergeTblSchema, err := mergeTbl.GetSchema(ctx)

	if err != nil {
		return nil, nil, err
	}

	ancTblSchema, err := ancTbl.GetSchema(ctx)

	if err != nil {
		return nil, nil, err
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

	ancRows, err := ancTbl.GetRowData(ctx)

	if err != nil {
		return nil, nil, err
	}

	updatedTbl, err := tbl.UpdateSchema(ctx, postMergeSchema)

	if err != nil {
		return nil, nil, err
	}

	err = tableEditSession.UpdateRoot(ctx, func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
		return root.PutTable(ctx, tblName, updatedTbl)
	})

	updatedTblEditor, err := tableEditSession.GetTableEditor(ctx, tblName, nil)

	if err != nil {
		return nil, nil, err
	}

	resultTbl, conflicts, stats, err := mergeTableData(ctx, tblName, postMergeSchema, rows, mergeRows, ancRows, merger.vrw, updatedTblEditor)

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

func mergeTableData(ctx context.Context, tblName string, sch schema.Schema, rows, mergeRows, ancRows types.Map, vrw types.ValueReadWriter, tblEdit *doltdb.SessionedTableEditor) (*doltdb.Table, types.Map, *MergeStats, error) {
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
					err = applyChange(ctx, tblEdit, rows, sch, stats, mergeChange)
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
					err = applyChange(ctx, tblEdit, rows, sch, stats, types.ValueChanged{ChangeType: change.ChangeType, Key: key, OldValue: r, NewValue: mergedRow})
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
	newRoot, err := tblEdit.Flush(ctx)
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

func applyChange(ctx context.Context, tableEditor *doltdb.SessionedTableEditor, rowData types.Map, sch schema.Schema, stats *MergeStats, change types.ValueChanged) error {
	switch change.ChangeType {
	case types.DiffChangeAdded:
		newRow, err := row.FromNoms(sch, change.Key.(types.Tuple), change.NewValue.(types.Tuple))
		if err != nil {
			return err
		}
		val, ok, err := rowData.MaybeGet(ctx, change.Key)
		if err != nil {
			return err
		} else if ok {
			oldRow, err := row.FromNoms(sch, change.Key.(types.Tuple), val.(types.Tuple))
			if err != nil {
				return err
			}
			err = tableEditor.UpdateRow(ctx, oldRow, newRow)
			if err != nil {
				return err
			}
		} else {
			err = tableEditor.InsertRow(ctx, newRow)
			if err != nil {
				return err
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
		err = tableEditor.UpdateRow(ctx, oldRow, newRow)
		if err != nil {
			return err
		}
		stats.Modifications++
	case types.DiffChangeRemoved:
		err := tableEditor.DeleteKey(ctx, change.Key.(types.Tuple))
		if err != nil {
			return err
		}
		stats.Deletes++
	}

	return nil
}

func rowMerge(ctx context.Context, nbf *types.NomsBinFormat, sch schema.Schema, r, mergeRow, baseRow types.Value) (types.Value, bool, error) {
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
	tableEditSession := doltdb.CreateTableEditSession(ourRoot, doltdb.TableEditSessionProps{
		ForeignKeyChecksDisabled: true,
	})
	var unconflicted []string
	// need to validate merges can be done on all tables before starting the actual merges.
	for _, tblName := range tblNames {
		mergedTable, stats, err := merger.MergeTable(ctx, tblName, tableEditSession)

		if err != nil {
			return nil, nil, err
		}

		if mergedTable != nil {
			tblToStats[tblName] = stats

			if stats.Conflicts == 0 {
				unconflicted = append(unconflicted, tblName)
			}

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
		return root.PutForeignKeyCollection(ctx, mergedFKColl)
	})

	err = tableEditSession.ValidateForeignKeys(ctx)
	if err != nil {
		return nil, nil, err
	}

	newRoot, err = newRoot.UpdateSuperSchemasFromOther(ctx, unconflicted, theirRoot)

	if err != nil {
		return nil, nil, err
	}

	return newRoot, tblToStats, nil
}

func GetTablesInConflict(ctx context.Context, dEnv *env.DoltEnv) (workingInConflict, stagedInConflict, headInConflict []string, err error) {
	var headRoot, stagedRoot, workingRoot *doltdb.RootValue

	headRoot, err = dEnv.HeadRoot(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	stagedRoot, err = dEnv.StagedRoot(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	workingRoot, err = dEnv.WorkingRoot(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	headInConflict, err = headRoot.TablesInConflict(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	stagedInConflict, err = stagedRoot.TablesInConflict(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	workingInConflict, err = workingRoot.TablesInConflict(ctx)

	if err != nil {
		return nil, nil, nil, err
	}

	return workingInConflict, stagedInConflict, headInConflict, err
}

func GetDocsInConflict(ctx context.Context, dEnv *env.DoltEnv) (*diff.DocDiffs, error) {
	docDetails, err := dEnv.GetAllValidDocDetails()
	if err != nil {
		return nil, err
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, err
	}

	return diff.NewDocDiffs(ctx, workingRoot, nil, docDetails)
}
