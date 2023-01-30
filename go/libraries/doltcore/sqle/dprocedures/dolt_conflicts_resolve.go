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
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/set"
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
	return rowToIter(res), nil
}

// DoltConflictsCatFunc runs a `dolt commit` in the SQL context, committing staged changes to head.
// Deprecated: please use the version in the dprocedures package
type DoltConflictsCatFunc struct {
	children []sql.Expression
}

func getProllyRowMaps(ctx *sql.Context, vrw types.ValueReadWriter, ns tree.NodeStore, hash hash.Hash, tblName string) (prolly.Map, error) {
	rootVal, err := doltdb.LoadRootValueFromRootIshAddr(ctx, vrw, ns, hash)
	tbl, ok, err := rootVal.GetTable(ctx, tblName)
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

func resolveProllyConflicts(ctx *sql.Context, tbl *doltdb.Table, tblName string, sch schema.Schema) (*doltdb.Table, error) {
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
	mutIdxs, err := merge.GetMutableSecondaryIdxs(ctx, sch, idxSet)
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

func validateConstraintViolations(ctx *sql.Context, before, after *doltdb.RootValue, table string) error {
	tables, err := after.GetTableNames(ctx)
	if err != nil {
		return err
	}

	violators, err := merge.GetForeignKeyViolatedTables(ctx, after, before, set.NewStrSet(tables))
	if err != nil {
		return err
	}
	if violators.Size() > 0 {
		return fmt.Errorf("resolving conflicts for table %s created foreign key violations", table)
	}

	return nil
}

func clearTableAndUpdateRoot(ctx *sql.Context, root *doltdb.RootValue, tbl *doltdb.Table, tblName string) (*doltdb.RootValue, error) {
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

func ResolveConflicts(ctx *sql.Context, dSess *dsess.DoltSession, root *doltdb.RootValue, dbName string, ours bool, tblNames []string) error {
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
			return nil
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
				tbl, err = resolveProllyConflicts(ctx, tbl, tblName, sch)
			} else {
				state, _, err := dSess.LookupDbState(ctx, dbName)
				if err != nil {
					return err
				}
				opts := state.WriteSession.GetOptions()
				tbl, err = resolveNomsConflicts(ctx, opts, tbl, tblName, sch)
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
	return dSess.SetRoot(ctx, dbName, root)
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
	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
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
	root := roots.Working
	tbls := apr.Args
	if len(tbls) == 1 && tbls[0] == "." {
		if allTables, err := root.TablesInConflict(ctx); err != nil {
			return 1, err
		} else {
			tbls = allTables
		}
	}

	err = ResolveConflicts(ctx, dSess, root, dbName, ours, tbls)
	if err != nil {
		return 1, err
	}

	return 0, nil
}
