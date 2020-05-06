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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type AutoResolver func(key types.Value, conflict doltdb.Conflict) (types.Value, error)

func Ours(key types.Value, cnf doltdb.Conflict) (types.Value, error) {
	return cnf.Value, nil
}

func Theirs(key types.Value, cnf doltdb.Conflict) (types.Value, error) {
	return cnf.MergeValue, nil
}

func ResolveTable(ctx context.Context, vrw types.ValueReadWriter, tbl *doltdb.Table, autoResFunc AutoResolver) (*doltdb.Table, error) {
	if has, err := tbl.HasConflicts(); err != nil {
		return nil, err
	} else if !has {
		return nil, doltdb.ErrNoConflicts
	}

	tblSchRef, err := tbl.GetSchemaRef()
	if err != nil {
		return nil, err
	}

	tblSchVal, err := tblSchRef.TargetValue(ctx, vrw)
	if err != nil {
		return nil, err
	}

	tblSch, err := encoding.UnmarshalSchemaNomsValue(ctx, vrw.Format(), tblSchVal)
	if err != nil {
		return nil, err
	}

	schemas, conflicts, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	rowEditor := rowData.Edit()

	indexes := tblSch.Indexes().AllIndexes()
	indexSchemas := make([]schema.Schema, tblSch.Indexes().Count())
	indexRowDatas := make([]*types.MapEditor, tblSch.Indexes().Count())
	for i, index := range indexes {
		indexSchemas[i] = index.Schema()
		indexRowData, err := tbl.GetIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		indexRowDatas[i] = indexRowData.Edit()
	}

	err = conflicts.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		cnf, err := doltdb.ConflictFromTuple(value.(types.Tuple))
		if err != nil {
			return false, err
		}

		originalRow, err := row.FromNoms(tblSch, key.(types.Tuple), cnf.Value.(types.Tuple))
		if err != nil {
			return false, err
		}

		updated, err := autoResFunc(key, cnf)
		if err != nil {
			return false, err
		}

		if types.IsNull(updated) {
			rowEditor.Remove(key)
			for i, index := range indexes {
				originalIndexRow, err := originalRow.ReduceToIndex(index)
				if err != nil {
					return false, err
				}
				indexRowDatas[i].Remove(originalIndexRow.NomsMapKey(indexSchemas[i]))
			}
		} else {
			r, err := row.FromNoms(tblSch, key.(types.Tuple), updated.(types.Tuple))
			if err != nil {
				return false, err
			}

			if has, err := row.IsValid(r, tblSch); err != nil {
				return false, err
			} else if !has {
				return false, table.NewBadRow(r)
			}

			rowEditor.Set(key, updated)

			for i, index := range indexes {
				indexSch := indexSchemas[i]
				originalIndexRow, err := originalRow.ReduceToIndex(index)
				if err != nil {
					return false, err
				}
				updatedIndexRow, err := r.ReduceToIndex(index)
				if err != nil {
					return false, err
				}
				if !row.AreEqual(originalIndexRow, updatedIndexRow, indexSch) {
					indexRowData := indexRowDatas[i]
					indexRowData.Remove(originalIndexRow.NomsMapKey(indexSch))
					indexRowData.Set(updatedIndexRow.NomsMapKey(indexSch), updatedIndexRow.NomsMapValue(indexSch))
				}
			}
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	m, err := rowEditor.Map(ctx)
	if err != nil {
		return nil, err
	}

	indexesUpdatedTable := tbl
	for i, index := range indexes {
		indexRowData, err := indexRowDatas[i].Map(ctx)
		if err != nil {
			return nil, err
		}
		indexesUpdatedTable, err = indexesUpdatedTable.SetIndexRowData(ctx, index.Name(), indexRowData)
		if err != nil {
			return nil, err
		}
	}

	indexData, err := indexesUpdatedTable.GetIndexData(ctx)
	if err != nil {
		return nil, err
	}

	newTbl, err := doltdb.NewTable(ctx, vrw, tblSchVal, m, &indexData)
	if err != nil {
		return nil, err
	}

	m, err = types.NewMap(ctx, vrw)
	if err != nil {
		return nil, err
	}

	newTbl, err = newTbl.SetConflicts(ctx, schemas, m)
	if err != nil {
		return nil, err
	}

	return newTbl, nil
}
