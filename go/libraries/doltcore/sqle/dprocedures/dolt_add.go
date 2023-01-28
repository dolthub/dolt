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
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"

	"github.com/dolthub/go-mysql-server/sql"
)

// doltAdd is the stored procedure version for the CLI command `dolt add`.
func doltAdd(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltAdd(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltAdd(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 1, err
	}

	apr, err := cli.CreateAddArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	allFlag := apr.Contains(cli.AllFlag)

	dSess := dsess.DSessFromSess(ctx.Session)
	roots, ok := dSess.GetRoots(ctx, dbName)
	if apr.NArg() == 0 && !allFlag {
		return 1, fmt.Errorf("Nothing specified, nothing added. Maybe you wanted to say 'dolt add .'?")
	} else if allFlag || apr.NArg() == 1 && apr.Arg(0) == "." {
		if !ok {
			return 1, fmt.Errorf("db session not found")
		}

		roots, err = actions.StageAllTables(ctx, roots)
		if err != nil {
			return 1, err
		}

		err = dSess.SetRoots(ctx, dbName, roots)
		if err != nil {
			return 1, err
		}
	} else {
		roots, err = actions.StageTables(ctx, roots, apr.Args)
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
