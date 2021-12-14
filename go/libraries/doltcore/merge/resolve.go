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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

type AutoResolver func(key types.Value, conflict conflict.Conflict) (types.Value, error)

func Ours(key types.Value, cnf conflict.Conflict) (types.Value, error) {
	return cnf.Value, nil
}

func Theirs(key types.Value, cnf conflict.Conflict) (types.Value, error) {
	return cnf.MergeValue, nil
}

func ResolveTable(ctx context.Context, vrw types.ValueReadWriter, tblName string, tbl *doltdb.Table, autoResFunc AutoResolver, sess *editor.TableEditSession) error {
	if has, err := tbl.HasConflicts(ctx); err != nil {
		return err
	} else if !has {
		return nil
	}

	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	if schema.IsKeyless(tblSch) {
		tbl, err = resolveKeylessTable(ctx, tbl, autoResFunc)
	} else {
		tbl, err = resolvePkTable(ctx, sess, tbl, tblName, autoResFunc)
	}
	if err != nil {
		return err
	}

	schemas, _, err := tbl.GetConflicts(ctx)
	if err != nil {
		return err
	}

	return sess.UpdateRoot(ctx, func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
		m, err := types.NewMap(ctx, vrw)
		if err != nil {
			return nil, err
		}

		tbl, err = tbl.SetConflicts(ctx, schemas, m)
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

		return root.PutTable(ctx, tblName, tbl)
	})
}

func resolvePkTable(ctx context.Context, sess *editor.TableEditSession, tbl *doltdb.Table, tblName string, auto AutoResolver) (*doltdb.Table, error) {
	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	_, conflicts, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	tableEditor, err := sess.GetTableEditor(ctx, tblName, tblSch)
	if err != nil {
		return nil, err
	}

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

	root, err := sess.Flush(ctx)
	if err != nil {
		return nil, err
	}

	newTbl, ok, err := root.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("resolved table `%s` cannot be found", tblName)
	}

	return newTbl, nil
}

func resolveKeylessTable(ctx context.Context, tbl *doltdb.Table, auto AutoResolver) (*doltdb.Table, error) {
	_, conflicts, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	rowData, err := tbl.GetRowData(ctx)
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

	return tbl.UpdateRows(ctx, rowData)
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
	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	tableEditSession := editor.CreateTableEditSession(root, opts)

	for _, tblName := range tbls {
		tbl, ok, err := root.GetTable(ctx, tblName)

		if err != nil {
			return err
		}

		if !ok {
			return doltdb.ErrTableNotFound
		}

		err = ResolveTable(ctx, root.VRW(), tblName, tbl, autoResolver, tableEditSession)

		if err != nil {
			return err
		}
	}

	newRoot, err := tableEditSession.Flush(ctx)
	if err != nil {
		return err
	}

	return dEnv.UpdateWorkingRoot(ctx, newRoot)
}
