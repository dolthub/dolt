package merge

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
)

var ErrUpToDate = errors.New("up to date")
var ErrFastForward = errors.New("fast forward")
var ErrSameTblAddedTwice = errors.New("table with same name added in 2 commits can't be merged")

type Merger struct {
	commit      *doltdb.Commit
	mergeCommit *doltdb.Commit
	ancestor    *doltdb.Commit
	vrw         types.ValueReadWriter
}

func NewMerger(commit, mergeCommit *doltdb.Commit, vrw types.ValueReadWriter) (*Merger, error) {
	ancestor, err := doltdb.GetCommitAnscestor(commit, mergeCommit)

	if err != nil {
		return nil, err
	}

	ancHash := ancestor.HashOf()
	hash := commit.HashOf()
	mergeHash := mergeCommit.HashOf()

	if hash == mergeHash || ancHash == mergeHash {
		return nil, ErrUpToDate
	} else if hash == mergeHash {
		return nil, ErrFastForward
	}

	return &Merger{commit, mergeCommit, ancestor, vrw}, nil
}

func (merger *Merger) MergeTable(tblName string) (*doltdb.Table, *MergeStats, error) {
	root := merger.commit.GetRootValue()
	mergeRoot := merger.mergeCommit.GetRootValue()
	ancRoot := merger.ancestor.GetRootValue()

	tbl, ok := root.GetTable(tblName)
	mergeTbl, mergeOk := mergeRoot.GetTable(tblName)
	ancTbl, ancOk := ancRoot.GetTable(tblName)

	if ok && mergeOk && tbl.HashOf() == mergeTbl.HashOf() {
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

	rows := tbl.GetRowData()
	mergeRows := mergeTbl.GetRowData()
	ancRows := ancTbl.GetRowData()

	mergedRowData, conflicts, stats, err := mergeTableData(rows, mergeRows, ancRows, merger.vrw)

	if err != nil {
		return nil, nil, err
	}

	mergedTable := tbl.UpdateRows(mergedRowData)

	if conflicts.Len() > 0 {
		schemas := doltdb.NewConflict(ancTbl.GetSchemaRef(), tbl.GetSchemaRef(), mergeTbl.GetSchemaRef())
		mergedTable = mergedTable.SetConflicts(schemas, conflicts)
	}

	return mergedTable, stats, nil
}

func stopAndDrain(stop chan<- struct{}, drain <-chan types.ValueChanged) {
	close(stop)
	for range drain {
	}
}

func mergeTableData(rows, mergeRows, ancRows types.Map, vrw types.ValueReadWriter) (types.Map, types.Map, *MergeStats, error) {
	//changeChan1, changeChan2 := make(chan diff.Difference, 32), make(chan diff.Difference, 32)
	changeChan, mergeChangeChan := make(chan types.ValueChanged, 32), make(chan types.ValueChanged, 32)
	stopChan, mergeStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		//diff.Diff(rows1, ancRows, changeChan1, stopChan1, true, dontDescend)
		rows.Diff(ancRows, changeChan, stopChan)
		close(changeChan)
	}()

	go func() {
		//diff.Diff(rows2, ancRows, changeChan2, stopChan2, true, dontDescend)
		mergeRows.Diff(ancRows, mergeChangeChan, mergeStopChan)
		close(mergeChangeChan)
	}()

	defer stopAndDrain(stopChan, changeChan)
	defer stopAndDrain(mergeStopChan, mergeChangeChan)

	conflictValChan := make(chan types.Value)
	conflictMapChan := types.NewStreamingMap(vrw, conflictValChan)
	mapEditor := rows.Edit()

	stats := &MergeStats{Operation: TableModified}
	var change, mergeChange types.ValueChanged
	for {
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

		if key != nil && (mergeKey == nil || key.Less(mergeKey)) {
			// change will already be in the map
			change = types.ValueChanged{}
		} else if mergeKey != nil && (key == nil || mergeKey.Less(key)) {
			applyChange(mapEditor, stats, mergeChange)
			mergeChange = types.ValueChanged{}
		} else {
			row, mergeRow, ancRow := change.NewValue, mergeChange.NewValue, change.OldValue
			mergedRow, isConflict := rowMerge(vrw, row, mergeRow, ancRow)

			if isConflict {
				stats.Conflicts++
				conflictTuple := doltdb.NewConflict(ancRow, row, mergeRow).ToNomsList(vrw)
				addConflict(conflictValChan, key, conflictTuple)
			} else {
				applyChange(mapEditor, stats, types.ValueChanged{change.ChangeType, key, row, mergedRow})
			}

			change = types.ValueChanged{}
			mergeChange = types.ValueChanged{}
		}
	}

	close(conflictValChan)
	conflicts := <-conflictMapChan
	mergedData := mapEditor.Map()

	return mergedData, conflicts, stats, nil
}

func addConflict(conflictChan chan types.Value, key types.Value, value types.List) {
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

func rowMerge(vrw types.ValueReadWriter, row, mergeRow, baseRow types.Value) (resultRow types.Value, isConflict bool) {
	if baseRow == nil {
		if row.Equals(mergeRow) {
			// same row added to both
			return row, false
		} else {
			// different rows added for the same key
			return nil, true
		}
	} else if row == nil && mergeRow == nil {
		// same row removed from both
		return nil, false
	} else if row == nil || mergeRow == nil {
		// removed from one and modified in another
		return nil, true
	} else {
		tuple := row.(types.List)
		mergeTuple := mergeRow.(types.List)
		baseTuple := baseRow.(types.List)

		numVals := tuple.Len()
		numMergeVals := mergeTuple.Len()
		numBaseVals := baseTuple.Len()
		maxLen := numVals
		if numMergeVals > maxLen {
			maxLen = numMergeVals
		}

		resultVals := make([]types.Value, maxLen)
		for i := uint64(0); i < maxLen; i++ {
			var baseVal types.Value = types.NullValue
			var val types.Value = types.NullValue
			var mergeVal types.Value = types.NullValue
			if i < numBaseVals {
				baseVal = baseTuple.Get(i)
			}
			if i < numVals {
				val = tuple.Get(i)
			}

			if i < numMergeVals {
				mergeVal = mergeTuple.Get(i)
			}

			if val.Equals(mergeVal) {
				resultVals[int(i)] = val
			} else {
				modified := !val.Equals(baseVal)
				mergeModified := !mergeVal.Equals(baseVal)
				switch {
				case modified && mergeModified:
					return nil, true
				case modified:
					resultVals[int(i)] = val
				default:
					resultVals[int(i)] = mergeVal
				}
			}
		}

		return types.NewList(vrw, resultVals...), false
	}
}
