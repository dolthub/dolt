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

package dprocedures

import (
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrConfSchIncompatible = errors.New("the conflict schema's columns are not equal to the current schema's columns, please resolve manually")

// doltConflictsResolve is the stored procedure version for the CLI command `dolt conflict resolve`.
func doltConflictsResolve(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := DoDoltConflictsResolve(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

// DoltConflictsCatFunc runs a `dolt commit` in the SQL context, committing staged changes to head.
// Deprecated: please use the version in the dprocedures package
type DoltConflictsCatFunc struct {
	children []sql.Expression
}

func getProllyRowMaps(ctx *sql.Context, vrw types.ValueReadWriter, ns tree.NodeStore, hash hash.Hash, tblName string) (prolly.Map, error) {
	rootVal, err := doltdb.LoadRootValueFromRootIshAddr(ctx, vrw, ns, hash)
	tbl, ok, err := rootVal.GetTable(ctx, doltdb.TableName{Name: tblName})
	if err != nil {
		return prolly.Map{}, err
	}
	if !ok {
		return prolly.Map{}, doltdb.ErrTableNotFound
	}

	idx, err := tbl.GetRowData(ctx)
	if err != nil {
		return prolly.Map{}, err
	}

	return durable.ProllyMapFromIndex(idx), nil
}

func resolveProllyConflicts(ctx *sql.Context, tbl *doltdb.Table, tblName string, ourSch, sch schema.Schema) (*doltdb.Table, error) {
	var err error
	artifactIdx, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}

	artifactMap := durable.ProllyMapFromArtifactIndex(artifactIdx)
	iter, err := artifactMap.IterAllConflicts(ctx)
	if err != nil {
		return nil, err
	}

	// get mutable prolly map
	ourIdx, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	ourMap := durable.ProllyMapFromIndex(ourIdx)
	mutMap := ourMap.Mutate()

	// get mutable secondary indexes
	idxSet, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	mutIdxs, err := merge.GetMutableSecondaryIdxs(ctx, ourSch, sch, tblName, idxSet)
	if err != nil {
		return nil, err
	}

	var theirRoot hash.Hash
	var theirMap prolly.Map
	for {
		cnfArt, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// reload if their root hash changes
		if theirRoot != cnfArt.TheirRootIsh {
			theirMap, err = getProllyRowMaps(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), cnfArt.TheirRootIsh, tblName)
			if err != nil {
				return nil, err
			}
			theirRoot = cnfArt.TheirRootIsh
		}

		// get row data
		var ourRow, theirRow val.Tuple
		err = ourMap.Get(ctx, cnfArt.Key, func(_, v val.Tuple) error {
			ourRow = v
			return nil
		})
		if err != nil {
			return nil, err
		}
		err = theirMap.Get(ctx, cnfArt.Key, func(_, v val.Tuple) error {
			theirRow = v
			return nil
		})
		if err != nil {
			return nil, err
		}

		// update row data
		if len(theirRow) == 0 {
			err = mutMap.Delete(ctx, cnfArt.Key)
		} else {
			err = mutMap.Put(ctx, cnfArt.Key, theirRow)
		}
		if err != nil {
			return nil, err
		}

		// update secondary indexes
		for _, mutIdx := range mutIdxs {
			if len(ourRow) == 0 {
				err = mutIdx.InsertEntry(ctx, cnfArt.Key, theirRow)
			} else if len(theirRow) == 0 {
				err = mutIdx.DeleteEntry(ctx, cnfArt.Key, ourRow)
			} else {
				err = mutIdx.UpdateEntry(ctx, cnfArt.Key, ourRow, theirRow)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	// Update table
	newMap, err := mutMap.Map(ctx)
	if err != nil {
		return nil, err
	}
	newIdx := durable.IndexFromProllyMap(newMap)
	newTbl, err := tbl.UpdateRows(ctx, newIdx)
	if err != nil {
		return nil, err
	}

	// Apply index set changes
	for _, mutIdx := range mutIdxs {
		m, err := mutIdx.Map(ctx)
		if err != nil {
			return nil, err
		}
		idxSet, err = idxSet.PutIndex(ctx, mutIdx.Name, durable.IndexFromProllyMap(m))
		if err != nil {
			return nil, err
		}
	}
	newTbl, err = newTbl.SetIndexSet(ctx, idxSet)
	if err != nil {
		return nil, err
	}

	return newTbl, nil
}

func resolvePkConflicts(ctx *sql.Context, opts editor.Options, tbl *doltdb.Table, tblName string, sch schema.Schema, conflicts types.Map) (*doltdb.Table, error) {
	// Create table editor
	tblEditor, err := editor.NewTableEditor(ctx, tbl, sch, tblName, opts)
	if err != nil {
		return nil, err
	}

	err = conflicts.Iter(ctx, func(key, val types.Value) (stop bool, err error) {
		k := key.(types.Tuple)
		cnf, err := conflict.ConflictFromTuple(val.(types.Tuple))
		if err != nil {
			return true, err
		}

		// row was removed
		if types.IsNull(cnf.MergeValue) {
			baseRow, err := row.FromNoms(sch, k, cnf.Base.(types.Tuple))
			if err != nil {
				return true, err
			}
			err = tblEditor.DeleteRow(ctx, baseRow)
			if err != nil {
				return true, err
			}
			return false, nil
		}

		newRow, err := row.FromNoms(sch, k, cnf.MergeValue.(types.Tuple))
		if err != nil {
			return true, err
		}

		if isValid, err := row.IsValid(newRow, sch); err != nil {
			return true, err
		} else if !isValid {
			return true, table.NewBadRow(newRow, "error resolving conflicts", fmt.Sprintf("row with primary key %v in table %s does not match constraints or types of the table's schema.", key, tblName))
		}

		// row was added
		if types.IsNull(cnf.Value) {
			err = tblEditor.InsertRow(ctx, newRow, nil)
			if err != nil {
				return true, err
			}
			return false, nil
		}

		// row was modified
		oldRow, err := row.FromNoms(sch, k, cnf.Value.(types.Tuple))
		if err != nil {
			return true, err
		}
		err = tblEditor.UpdateRow(ctx, oldRow, newRow, nil)
		if err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return tblEditor.Table(ctx)
}

func resolveKeylessConflicts(ctx *sql.Context, tbl *doltdb.Table, conflicts types.Map) (*doltdb.Table, error) {
	rowData, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	mapEditor := rowData.Edit()
	err = conflicts.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		cnf, err := conflict.ConflictFromTuple(value.(types.Tuple))
		if err != nil {
			return true, err
		}

		if types.IsNull(cnf.MergeValue) {
			mapEditor.Remove(key)
		} else {
			mapEditor.Set(key, cnf.MergeValue)
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	rowData, err = mapEditor.Map(ctx)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateNomsRows(ctx, rowData)
}

func resolveNomsConflicts(ctx *sql.Context, opts editor.Options, tbl *doltdb.Table, tblName string, sch schema.Schema) (*doltdb.Table, error) {
	// Get conflicts
	_, confIdx, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}
	conflicts := durable.NomsMapFromConflictIndex(confIdx)

	if schema.IsKeyless(sch) {
		return resolveKeylessConflicts(ctx, tbl, conflicts)
	}

	return resolvePkConflicts(ctx, opts, tbl, tblName, sch, conflicts)
}

func validateConstraintViolations(ctx *sql.Context, before, after doltdb.RootValue, table doltdb.TableName) error {
	tables, err := after.GetTableNames(ctx, table.Schema)
	if err != nil {
		return err
	}

	violators, err := merge.GetForeignKeyViolatedTables(ctx, after, before, doltdb.NewTableNameSet(doltdb.ToTableNames(tables, table.Schema)))
	if err != nil {
		return err
	}
	if violators.Size() > 0 {
		return fmt.Errorf("resolving conflicts for table %s created foreign key violations", table)
	}

	return nil
}

func clearTableAndUpdateRoot(ctx *sql.Context, root doltdb.RootValue, tbl *doltdb.Table, tblName doltdb.TableName) (doltdb.RootValue, error) {
	newTbl, err := tbl.ClearConflicts(ctx)
	if err != nil {
		return nil, err
	}
	newRoot, err := root.PutTable(ctx, tblName, newTbl)
	if err != nil {
		return nil, err
	}
	return newRoot, nil
}

func ResolveSchemaConflicts(ctx *sql.Context, ddb *doltdb.DoltDB, ws *doltdb.WorkingSet, resolveOurs bool, tblNames []doltdb.TableName) (*doltdb.WorkingSet, error) {
	if !ws.MergeActive() {
		return ws, nil // no schema conflicts
	}

	// TODO: There's an issue with using `dolt conflicts resolve` for schema conflicts, since having
	//
	//	schema conflicts reported means that we haven't yet merged the table data. In some case,
	//	such as when there have ONLY been schema changes and no data changes that need to be
	//	merged, it is safe to use `dolt conflicts resolve`, but there are many other cases where the
	//	data changes would not be merged and could surprise customers. So, we are being cautious to
	//	prevent auto-resolution of schema changes with `dolt conflicts resolve` until we have a fix
	//	for resolving schema changes AND merging data (including dealing with any data conflicts).
	//	For more details, see: https://github.com/dolthub/dolt/issues/6616
	if ws.MergeState().HasSchemaConflicts() {
		return nil, fmt.Errorf("Unable to automatically resolve schema conflicts since data changes may " +
			"not have been fully merged yet. " +
			"To continue, abort this merge (dolt merge --abort) then apply ALTER TABLE statements to one " +
			"side of this merge to get the two schemas in sync with the desired schema, then rerun the merge. " +
			"To track resolution of this limitation, follow https://github.com/dolthub/dolt/issues/6616")
	}

	tblSet := doltdb.NewTableNameSet(tblNames)
	updates := make(map[doltdb.TableName]*doltdb.Table)
	err := ws.MergeState().IterSchemaConflicts(ctx, ddb, func(table doltdb.TableName, conflict doltdb.SchemaConflict) error {
		if !tblSet.Contains(table) {
			return nil
		}
		ours, theirs := conflict.GetConflictingTables()
		if resolveOurs {
			updates[table] = ours
		} else {
			updates[table] = theirs
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var merged []doltdb.TableName
	root := ws.WorkingRoot()
	for name, tbl := range updates {
		if root, err = root.PutTable(ctx, name, tbl); err != nil {
			return nil, err
		}
		merged = append(merged, name)
	}

	// clear resolved schema conflicts
	var unmerged []doltdb.TableName
	for _, tbl := range ws.MergeState().TablesWithSchemaConflicts() {
		if tblSet.Contains(tbl) {
			continue
		}
		unmerged = append(unmerged, tbl)
	}

	return ws.WithWorkingRoot(root).WithUnmergableTables(unmerged).WithMergedTables(merged), nil
}

func ResolveDataConflicts(ctx *sql.Context, dSess *dsess.DoltSession, root doltdb.RootValue, dbName string, ours bool, tblNames []doltdb.TableName) error {
	for _, tblName := range tblNames {
		tbl, ok, err := root.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if !ok {
			return doltdb.ErrTableNotFound
		}

		if has, err := tbl.HasConflicts(ctx); err != nil {
			return err
		} else if !has {
			continue
		}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return err
		}
		_, ourSch, theirSch, err := tbl.GetConflictSchemas(ctx, tblName)
		if err != nil {
			return err
		}

		if ours && !schema.ColCollsAreEqual(sch.GetAllCols(), ourSch.GetAllCols()) {
			return ErrConfSchIncompatible
		} else if !ours && !schema.ColCollsAreEqual(sch.GetAllCols(), theirSch.GetAllCols()) {
			return ErrConfSchIncompatible
		}

		if !ours {
			if tbl.Format() == types.Format_DOLT {
				tbl, err = resolveProllyConflicts(ctx, tbl, tblName.Name, ourSch, sch)
			} else {
				state, _, err := dSess.LookupDbState(ctx, dbName)
				if err != nil {
					return err
				}
				var opts editor.Options
				if ws := state.WriteSession(); ws != nil {
					opts = ws.GetOptions()
				}
				tbl, err = resolveNomsConflicts(ctx, opts, tbl, tblName.Name, sch)
			}
			if err != nil {
				return err
			}
		}

		newRoot, err := clearTableAndUpdateRoot(ctx, root, tbl, tblName)
		if err != nil {
			return err
		}

		err = validateConstraintViolations(ctx, root, newRoot, tblName)
		if err != nil {
			return err
		}

		root = newRoot
	}
	return dSess.SetWorkingRoot(ctx, dbName, root)
}

func DoDoltConflictsResolve(ctx *sql.Context, args []string) (int, error) {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 1, err
	}
	dbName := ctx.GetCurrentDatabase()

	apr, err := cli.CreateConflictsResolveArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return 0, err
	}

	ddb, _ := dSess.GetDoltDB(ctx, dbName)
	if err != nil {
		return 0, err
	}

	ours := apr.Contains(cli.OursFlag)
	theirs := apr.Contains(cli.TheirsFlag)
	if ours && theirs {
		return 1, fmt.Errorf("specify only either --ours or --theirs")
	} else if !ours && !theirs {
		return 1, fmt.Errorf("--ours or --theirs must be supplied")
	}

	if apr.NArg() == 0 {
		return 1, fmt.Errorf("specify at least one table to resolve conflicts")
	}

	// get all tables in conflict
	strTableNames := apr.Args
	var tableNames []doltdb.TableName

	if len(strTableNames) == 1 && strTableNames[0] == "." {
		all := actions.GetAllTableNames(ctx, ws.WorkingRoot())
		if err != nil {
			return 1, nil
		}
		tableNames = all
	} else {
		for _, tblName := range strTableNames {
			tn, _, ok, err := resolve.Table(ctx, ws.WorkingRoot(), tblName)
			if err != nil {
				return 1, nil
			}
			if !ok {
				return 1, doltdb.ErrTableNotFound
			}
			tableNames = append(tableNames, tn)
		}
	}

	ws, err = ResolveSchemaConflicts(ctx, ddb, ws, ours, tableNames)
	if err != nil {
		return 1, err
	}

	err = ResolveDataConflicts(ctx, dSess, ws.WorkingRoot(), dbName, ours, tableNames)
	if err != nil {
		return 1, err
	}

	return 0, nil
}
