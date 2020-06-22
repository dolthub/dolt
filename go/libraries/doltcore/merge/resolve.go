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
	"fmt"

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

func ResolveTable(ctx context.Context, vrw types.ValueReadWriter, tblName string, tbl *doltdb.Table, autoResFunc AutoResolver, tableEditSession *doltdb.TableEditSession) error {
	if has, err := tbl.HasConflicts(); err != nil {
		return err
	} else if !has {
		return doltdb.ErrNoConflicts
	}

	tableEditor, err := tableEditSession.GetTableEditor(ctx, tblName, nil)
	if err != nil {
		return err
	}

	tblSchRef, err := tbl.GetSchemaRef()
	if err != nil {
		return err
	}

	tblSchVal, err := tblSchRef.TargetValue(ctx, vrw)
	if err != nil {
		return err
	}

	tblSch, err := encoding.UnmarshalSchemaNomsValue(ctx, vrw.Format(), tblSchVal)
	if err != nil {
		return err
	}

	schemas, conflicts, err := tbl.GetConflicts(ctx)
	if err != nil {
		return err
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

		var updatedRow row.Row

		if types.IsNull(updated) {
			err := tableEditor.DeleteKey(ctx, key.(types.Tuple))
			if err != nil {
				return false, err
			}
		} else {
			updatedRow, err = row.FromNoms(tblSch, key.(types.Tuple), updated.(types.Tuple))
			if err != nil {
				return false, err
			}

			if has, err := row.IsValid(updatedRow, tblSch); err != nil {
				return false, err
			} else if !has {
				return false, table.NewBadRow(updatedRow)
			}

			err := tableEditor.UpdateRow(ctx, originalRow, updatedRow)
			if err != nil {
				return false, err
			}
		}

		return false, nil
	})
	if err != nil {
		return err
	}

	newRoot, err := tableEditSession.GetRoot(ctx)
	if err != nil {
		return err
	}

	newTbl, ok, err := newRoot.GetTable(ctx, tblName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("resolved table `%s` cannot be found", tblName)
	}

	m, err := types.NewMap(ctx, vrw)
	if err != nil {
		return err
	}

	newTbl, err = newTbl.SetConflicts(ctx, schemas, m)
	if err != nil {
		return err
	}

	newRoot, err = newRoot.PutTable(ctx, tblName, newTbl)
	if err != nil {
		return err
	}

	return tableEditSession.SetRoot(ctx, newRoot)
}
