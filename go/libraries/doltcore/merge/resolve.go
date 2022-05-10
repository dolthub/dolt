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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

type AutoResolver func(key types.Value, conflict conflict.Conflict) (types.Value, error)

func Ours(key types.Value, cnf conflict.Conflict) (types.Value, error) {
	return cnf.Value, nil
}

func Theirs(key types.Value, cnf conflict.Conflict) (types.Value, error) {
	return cnf.MergeValue, nil
}

func ResolveTable(ctx context.Context, vrw types.ValueReadWriter, tblName string, root *doltdb.RootValue, autoResFunc AutoResolver, opts editor.Options) (*doltdb.RootValue, error) {
	tbl, ok, err := root.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	if has, err := tbl.HasConflicts(ctx); err != nil {
		return nil, err
	} else if !has {
		return root, nil
	}

	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(tblSch) {
		tbl, err = resolveKeylessTable(ctx, tbl, autoResFunc)
	} else {
		tbl, err = resolvePkTable(ctx, tbl, tblName, opts, autoResFunc)
	}
	if err != nil {
		return nil, err
	}

	schemas, _, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	confIdx, err := durable.NewEmptyConflictIndex(ctx, vrw, schemas.Schema, schemas.MergeSchema, schemas.Base)
	if err != nil {
		return nil, err
	}

	tbl, err = tbl.SetConflicts(ctx, schemas, confIdx)
	if err != nil {
		return nil, err
	}

	numRowsInConflict, err := tbl.NumRowsInConflict(ctx)
	if err != nil {
		return nil, err
	}

	if numRowsInConflict == 0 {
		tbl, err = tbl.ClearConflicts(ctx)
		if err != nil {
			return nil, err
		}
	}

	newRoot, err := root.PutTable(ctx, tblName, tbl)
	if err != nil {
		return nil, err
	}

	err = validateConstraintViolations(ctx, root, newRoot, tblName)
	if err != nil {
		return nil, err
	}

	return newRoot, nil
}

func resolvePkTable(ctx context.Context, tbl *doltdb.Table, tblName string, opts editor.Options, auto AutoResolver) (*doltdb.Table, error) {
	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	_, confIdx, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	tableEditor, err := editor.NewTableEditor(ctx, tbl, tblSch, tblName, opts)
	if err != nil {
		return nil, err
	}

	if confIdx.Format() == types.Format_DOLT_1 {
		panic("resolvePkTable not implemented for new storage format")
	}

	conflicts := durable.NomsMapFromConflictIndex(confIdx)
	err = conflicts.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		cnf, err := conflict.ConflictFromTuple(value.(types.Tuple))
		if err != nil {
			return false, err
		}

		updated, err := auto(key, cnf)
		if err != nil {
			return false, err
		}

		if types.IsNull(updated) {
			originalRow, err := row.FromNoms(tblSch, key.(types.Tuple), cnf.Base.(types.Tuple))
			if err != nil {
				return false, err
			}

			err = tableEditor.DeleteRow(ctx, originalRow)
			if err != nil {
				return false, err
			}
		} else {
			updatedRow, err := row.FromNoms(tblSch, key.(types.Tuple), updated.(types.Tuple))
			if err != nil {
				return false, err
			}

			if isValid, err := row.IsValid(updatedRow, tblSch); err != nil {
				return false, err
			} else if !isValid {
				return false, table.NewBadRow(updatedRow, "error resolving conflicts", fmt.Sprintf("row with primary key %v in table %s does not match constraints or types of the table's schema.", key, tblName))
			}

			if types.IsNull(cnf.Value) {
				err = tableEditor.InsertRow(ctx, updatedRow, nil)
				if err != nil {
					return false, err
				}
			} else {
				originalRow, err := row.FromNoms(tblSch, key.(types.Tuple), cnf.Value.(types.Tuple))
				if err != nil {
					return false, err
				}
				err = tableEditor.UpdateRow(ctx, originalRow, updatedRow, nil)
				if err != nil {
					return false, err
				}
			}
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return tableEditor.Table(ctx)
}

func resolveKeylessTable(ctx context.Context, tbl *doltdb.Table, auto AutoResolver) (*doltdb.Table, error) {
	_, confIdx, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	if confIdx.Format() == types.Format_DOLT_1 {
		panic("resolvePkTable not implemented for new storage format")
	}

	conflicts := durable.NomsMapFromConflictIndex(confIdx)

	rowData, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	edit := rowData.Edit()

	err = conflicts.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		cnf, err := conflict.ConflictFromTuple(value.(types.Tuple))
		if err != nil {
			return false, err
		}

		resolved, err := auto(key, cnf)
		if err != nil {
			return false, err
		}

		if types.IsNull(resolved) {
			edit.Remove(key)
		} else {
			edit.Set(key, resolved)
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	rowData, err = edit.Map(ctx)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateNomsRows(ctx, rowData)
}

func validateConstraintViolations(ctx context.Context, before, after *doltdb.RootValue, table string) error {
	tables, err := after.GetTableNames(ctx)
	if err != nil {
		return err
	}

	_, violators, err := AddConstraintViolations(ctx, after, before, set.NewStrSet(tables))
	if err != nil {
		return err
	}
	if violators.Size() > 0 {
		return fmt.Errorf("resolving conflicts for table %s created foreign key violations", table)
	}

	return nil
}

type AutoResolveStats struct {
}

func AutoResolveAll(ctx context.Context, dEnv *env.DoltEnv, autoResolver AutoResolver) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return err
	}

	tbls, err := root.TablesInConflict(ctx)

	if err != nil {
		return err
	}

	return autoResolve(ctx, dEnv, root, autoResolver, tbls)
}

func AutoResolveTables(ctx context.Context, dEnv *env.DoltEnv, autoResolver AutoResolver, tbls []string) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return err
	}

	return autoResolve(ctx, dEnv, root, autoResolver, tbls)
}

func autoResolve(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, autoResolver AutoResolver, tbls []string) error {
	var err error
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}
	for _, tblName := range tbls {
		root, err = ResolveTable(ctx, root.VRW(), tblName, root, autoResolver, opts)
		if err != nil {
			return err
		}
	}

	return dEnv.UpdateWorkingRoot(ctx, root)
}
