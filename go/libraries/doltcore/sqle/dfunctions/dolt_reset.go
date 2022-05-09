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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltResetFuncName = "dolt_reset"

// Deprecated: please use the version in the dprocedures package
type DoltResetFunc struct {
	children []sql.Expression
}

func (d DoltResetFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltReset(ctx, args)
}

func DoDoltReset(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)

	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	apr, err := cli.CreateResetArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	// Check if problems with args first.
	if apr.ContainsAll(cli.HardResetParam, cli.SoftResetParam) {
		return 1, fmt.Errorf("error: --%s and --%s are mutually exclusive options.", cli.HardResetParam, cli.SoftResetParam)
	}

	// Get all the needed roots.
	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	if apr.Contains(cli.HardResetParam) {
		// Get the commitSpec for the branch if it exists
		arg := ""
		if apr.NArg() > 1 {
			return 1, fmt.Errorf("--hard supports at most one additional param")
		} else if apr.NArg() == 1 {
			arg = apr.Arg(0)
		}

		var newHead *doltdb.Commit
		newHead, roots, err = actions.ResetHardTables(ctx, dbData, arg, roots)
		if err != nil {
			return 1, err
		}

		// TODO: this overrides the transaction setting, needs to happen at commit, not here
		if newHead != nil {
			if err := dbData.Ddb.SetHeadToCommit(ctx, dbData.Rsr.CWBHeadRef(), newHead); err != nil {
				return 1, err
			}
		}

		ws, err := dSess.WorkingSet(ctx, dbName)
		if err != nil {
			return 1, err
		}
		err = dSess.SetWorkingSet(ctx, dbName, ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged).ClearMerge())
		if err != nil {
			return 1, err
		}
	} else {
		roots, err = actions.ResetSoftTables(ctx, dbData, apr, roots)
		if err != nil {
			return 1, err
		}

		err = dSess.SetRoots(ctx, dbName, roots)
		if err != nil {
			return 1, err
		}
	}

	return 0, nil
}

func (d DoltResetFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltResetFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for i, child := range d.children {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_RESET(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltResetFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltResetFunc) IsNullable() bool {
	for _, child := range d.Children() {
		if child.IsNullable() {
			return true
		}
	}
	return false
}

func (d DoltResetFunc) Children() []sql.Expression {
	return d.children
}

func (d DoltResetFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltResetFunc(children...)
}

// Deprecated: please use the version in the dprocedures package
func NewDoltResetFunc(args ...sql.Expression) (sql.Expression, error) {
	return DoltResetFunc{children: args}, nil
}
