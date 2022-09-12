// Copyright 2020 Dolthub, Inc.
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

package dfunctions

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrConfSchIncompatible = errors.New("the conflict schema's columns are not equal to the current schema's columns, please resolve manually")

const DoltConflictsResolveFuncName = "dolt_conflicts_resolve"

// DoltConflictsCatFunc runs a `dolt commit` in the SQL context, committing staged changes to head.
// Deprecated: please use the version in the dprocedures package
type DoltConflictsCatFunc struct {
	children []sql.Expression
}

// NewDoltConflictsResolveFunc creates a new DoltCommitFunc expression whose children represents the args passed in DOLT_CONFLICTS_RESOLVE.
// Deprecated: please use the version in the dprocedures package
func NewDoltConflictsResolveFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltConflictsCatFunc{children: args}, nil
}

func (d DoltConflictsCatFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltConflictsResolve(ctx, args)
}

func AutoResolveTables(ctx *sql.Context, dSess *dsess.DoltSession, root *doltdb.RootValue, dbName string, ours bool, tblNames []string) error {
	newRoot := root
	for _, tblName := range tblNames {
		tbl, ok, err := newRoot.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if !ok {
			return doltdb.ErrTableNotFound
		}

		//has, err := tbl.HasConflicts(ctx)
		//if err != nil {
		//	return err
		//}
		//if !has {
		//	continue
		//}
		//
		//sch, err := tbl.GetSchema(ctx)
		//if err != nil {
		//	return err
		//}
		//
		_, _, theirSch, err := tbl.GetConflictSchemas(ctx, tblName)
		if err != nil {
			return err
		}
		//
		//conflictSchema, conflictIndex, err := tbl.GetConflicts(ctx)
		//if conflictSchema.Base == nil {
		//}
		//if conflictIndex == nil {
		//}

		cnfReader, err := merge.NewConflictReader(ctx, tbl)
		if err != nil {
		}

		joiner := cnfReader.GetJoiner()

		var pkVals []types.Value
		for {
			cnfRow, _, err := cnfReader.NextConflict(ctx)
			if err == io.EOF {
				break
			}
			cnfMap, err := joiner.Split(cnfRow)
			if err != nil {
				return err
			}

			vrw := tbl.ValueReadWriter()
			var pkVal types.Value
			if ours {
				row := cnfMap["their"]
				k := row.NomsMapKey(theirSch)
				v := row.NomsMapValue(theirSch)

				kv, _ := k.Value(ctx)
				vv, _ := v.Value(ctx)

				newMap, _ := types.NewMap(ctx, vrw, kv, vv)

				updatedTable, _ := tbl.UpdateNomsRows(ctx, newMap)
				pkVals = append(pkVals, kv)
				_, _, updatedTbl, _ := updatedTable.ResolveConflicts(ctx, pkVals)
				newRoot, _ = newRoot.PutTable(ctx, tblName, updatedTbl)
				return dSess.SetRoot(ctx, dbName, newRoot)
			} else {
				row := cnfMap["their"]
				pkVal, err = row.NomsMapKey(theirSch).Value(ctx)
				aa, _ := pkVal.Value(ctx)
				panic(fmt.Sprintf("%v, %v, %v", pkVal, aa, row))
			}
			if err != nil {
				return err
			}
			pkVals = append(pkVals, pkVal)
		}

		// TODO: problem is that this always picks ours...
		_, _, updatedTbl, err := tbl.ResolveConflicts(ctx, pkVals)
		if err != nil {
			if errors.Is(err, doltdb.ErrNoConflictsResolved) {
				return nil
			}
			return err
		}

		newRoot, err = newRoot.PutTable(ctx, tblName, updatedTbl)
		if err != nil {
			return err
		}

		//if ours && !schema.ColCollsAreEqual(sch.GetAllCols(), ourSch.GetAllCols()) {
		//	return ErrConfSchIncompatible
		//} else if !ours && !schema.ColCollsAreEqual(sch.GetAllCols(), theirSch.GetAllCols()) {
		//	return ErrConfSchIncompatible
		//}
	}
	return dSess.SetRoot(ctx, dbName, newRoot)
}

func DoDoltConflictsResolve(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()
	fmt.Printf("database name: %s", dbName)

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

	err = AutoResolveTables(ctx, dSess, root, dbName, ours, tbls)
	if err != nil {
		return 1, err
	}

	return 0, nil
}

func (d DoltConflictsCatFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for _, child := range d.children {
		childrenStrings = append(childrenStrings, child.String())
	}
	return fmt.Sprintf("DOLT_CONFLICTS_RESOLVE(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltConflictsCatFunc) Type() sql.Type {
	return sql.Text
}

func (d DoltConflictsCatFunc) IsNullable() bool {
	return false
}

func (d DoltConflictsCatFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltConflictsResolveFunc(children...)
}

func (d DoltConflictsCatFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltConflictsCatFunc) Children() []sql.Expression {
	return d.children
}
