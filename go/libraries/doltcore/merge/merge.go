// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/hash"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed"
	"github.com/liquidata-inc/dolt/go/libraries/utils/valutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ErrFastForward = errors.New("fast forward")
var ErrSameTblAddedTwice = errors.New("table with same name added in 2 commits can't be merged")

type Merger struct {
	commit      *doltdb.Commit
	mergeCommit *doltdb.Commit
	ancestor    *doltdb.Commit
	vrw         types.ValueReadWriter
}

func NewMerger(ctx context.Context, commit, mergeCommit *doltdb.Commit, vrw types.ValueReadWriter) (*Merger, error) {
	ancestor, err := doltdb.GetCommitAncestor(ctx, commit, mergeCommit)

	if err != nil {
		return nil, err
	}

	ff, err := commit.CanFastForwardTo(ctx, mergeCommit)
	if err != nil {
		return nil, err
	} else if ff {
		return nil, ErrFastForward
	}
	return &Merger{commit, mergeCommit, ancestor, vrw}, nil
}

func (merger *Merger) MergeTable(ctx context.Context, tblName string) (*doltdb.Table, *MergeStats, error) {
	root, err := merger.commit.GetRootValue()

	if err != nil {
		return nil, nil, err
	}

	mergeRoot, err := merger.mergeCommit.GetRootValue()

	if err != nil {
		return nil, nil, err
	}

	ancRoot, err := merger.ancestor.GetRootValue()

	if err != nil {
		return nil, nil, err
	}

	tbl, ok, err := root.GetTable(ctx, tblName)

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

	mergeTbl, mergeOk, err := mergeRoot.GetTable(ctx, tblName)

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

	ancTbl, ancOk, err := ancRoot.GetTable(ctx, tblName)

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
		return mergeTbl, &MergeStats{Operation: TableModified}, nil
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

	postMergeSchema, err := mergeTableSchema(tblSchema, mergeTblSchema, ancTblSchema)

	if err != nil {
		return nil, nil, err
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

	mergedRowData, conflicts, stats, err := mergeTableData(ctx, postMergeSchema, rows, mergeRows, ancRows, merger.vrw)

	if err != nil {
		return nil, nil, err
	}

	schUnionVal, err := encoding.MarshalSchemaAsNomsValue(ctx, merger.vrw, postMergeSchema)

	if err != nil {
		return nil, nil, err
	}

	mergedTable, err := doltdb.NewTable(ctx, merger.vrw, schUnionVal, mergedRowData)

	if err != nil {
		return nil, nil, err
	}

	if conflicts.Len() > 0 {

		if err != nil {
			return nil, nil, err
		}

		if err != nil {
			return nil, nil, err
		}

		if err != nil {
			return nil, nil, err
		}

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
		mergedTable, err = mergedTable.SetConflicts(ctx, schemas, conflicts)
	}

	return mergedTable, stats, nil
}

func stopAndDrain(stop chan<- struct{}, drain <-chan types.ValueChanged) {
	close(stop)
	for range drain {
	}
}

func mergeTableSchema(sch, mergeSch, ancSch schema.Schema) (schema.Schema, error) {
	// (sch - ancSch) ∪ (mergeSch - ancSch) ∪ (sch ∩ mergeSch)

	// columns remaining on both branches since the common ancestor
	intersection, err := typed.TypedColCollectionIntersection(sch, mergeSch)

	if err != nil {
		return nil, err
	}

	// columns added on the main branch since the common ancestor
	sub, err := typed.TypedColCollectionSubtraction(sch, ancSch)

	if err != nil {
		return nil, err
	}

	// columns added on the merge branch since the common ancestor
	mergeSub, err := typed.TypedColCollectionSubtraction(mergeSch, ancSch)

	if err != nil {
		return nil, err
	}

	// order of args here is important for correct column ordering in merged schema
	// TODO: column ordering will break if a column added on sub or merge was reordered
	// to be before any column in the intersection
	union, err := typed.TypedColCollUnion(intersection, sub, mergeSub)

	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(union), nil
}

func mergeTableData(ctx context.Context, sch schema.Schema, rows, mergeRows, ancRows types.Map, vrw types.ValueReadWriter) (types.Map, types.Map, *MergeStats, error) {
	//changeChan1, changeChan2 := make(chan diff.Difference, 32), make(chan diff.Difference, 32)
	ae := atomicerr.New()
	changeChan, mergeChangeChan := make(chan types.ValueChanged, 32), make(chan types.ValueChanged, 32)
	stopChan, mergeStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		//diff.Diff(rows1, ancRows, changeChan1, stopChan1, true, dontDescend)
		rows.Diff(ctx, ancRows, ae, changeChan, stopChan)
		close(changeChan)
	}()

	go func() {
		//diff.Diff(rows2, ancRows, changeChan2, stopChan2, true, dontDescend)
		mergeRows.Diff(ctx, ancRows, ae, mergeChangeChan, mergeStopChan)
		close(mergeChangeChan)
	}()

	defer stopAndDrain(stopChan, changeChan)
	defer stopAndDrain(mergeStopChan, mergeChangeChan)

	conflictValChan := make(chan types.Value)
	conflictMapChan := types.NewStreamingMap(ctx, vrw, ae, conflictValChan)
	mapEditor := rows.Edit()
	stats := &MergeStats{Operation: TableModified}

	f := func() error {
		defer close(conflictValChan)

		var change, mergeChange types.ValueChanged
		for {
			if ae.IsSet() {
				break
			}

			// Get the next change from both a and b. If either diff(a, parent) or diff(b, parent) is complete, aChange or bChange will get an empty types.ValueChanged containing a nil Value. Generally, though, this allows us to proceed through both diffs in (key) order, considering the "current" change from both diffs at the same time.
			if change.Key == nil {
				change = <-changeChan
			}
			if mergeChange.Key == nil {
				mergeChange = <-mergeChangeChan
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
					applyChange(mapEditor, stats, mergeChange)
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

					addConflict(conflictValChan, key, conflictTuple)
				} else {
					applyChange(mapEditor, stats, types.ValueChanged{ChangeType: change.ChangeType, Key: key, OldValue: r, NewValue: mergedRow})
				}

				change = types.ValueChanged{}
				mergeChange = types.ValueChanged{}
			}
		}

		return nil
	}

	err := f()

	if err != nil {
		return types.EmptyMap, types.EmptyMap, nil, err
	}

	if err := ae.Get(); err != nil {
		return types.EmptyMap, types.EmptyMap, nil, err
	}

	conflicts := <-conflictMapChan
	mergedData, err := mapEditor.Map(ctx)

	if err != nil {
		return types.EmptyMap, types.EmptyMap, nil, err
	}

	return mergedData, conflicts, stats, nil
}

func addConflict(conflictChan chan types.Value, key types.Value, value types.Tuple) {
	conflictChan <- key
	conflictChan <- value
}

func applyChange(me *types.MapEditor, stats *MergeStats, change types.ValueChanged) {
	switch change.ChangeType {
	case types.DiffChangeAdded:
		stats.Adds++
		me.Set(change.Key, change.NewValue)
	case types.DiffChangeModified:
		stats.Modifications++
		me.Set(change.Key, change.NewValue)
	case types.DiffChangeRemoved:
		stats.Deletes++
		me.Remove(change.Key)
	}
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

	tpl := resultVals.NomsTupleForTags(nbf, sch.GetNonPKCols().SortedTags, false)
	v, err := tpl.Value(ctx)

	if err != nil {
		return nil, false, err
	}

	return v, false, nil
}

func MergeCommits(ctx context.Context, ddb *doltdb.DoltDB, cm1, cm2 *doltdb.Commit) (*doltdb.RootValue, map[string]*MergeStats, error) {
	merger, err := NewMerger(ctx, cm1, cm2, ddb.ValueReadWriter())

	if err != nil {
		return nil, nil, err
	}

	root, err := cm1.GetRootValue()

	if err != nil {
		return nil, nil, err
	}

	rv, err := cm2.GetRootValue()

	if err != nil {
		return nil, nil, err
	}

	tblNames, err := doltdb.UnionTableNames(ctx, root, rv)

	if err != nil {
		return nil, nil, err
	}

	tblToStats := make(map[string]*MergeStats)

	// need to validate merges can be done on all tables before starting the actual merges.
	for _, tblName := range tblNames {
		mergedTable, stats, err := merger.MergeTable(ctx, tblName)

		if err != nil {
			return nil, nil, err
		}

		if mergedTable != nil {
			tblToStats[tblName] = stats

			var err error
			root, err = root.PutTable(ctx, tblName, mergedTable)

			if err != nil {
				return nil, nil, err
			}
		} else if has, err := root.HasTable(ctx, tblName); err != nil {
			return nil, nil, err
		} else if has {
			tblToStats[tblName] = &MergeStats{Operation: TableRemoved}
			root, err = root.RemoveTables(ctx, tblName)

			if err != nil {
				return nil, nil, err
			}
		} else {
			panic("?")
		}
	}

	return root, tblToStats, nil
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

	return diff.NewDocDiffs(ctx, dEnv, workingRoot, nil, docDetails)
}
