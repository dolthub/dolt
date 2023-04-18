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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// doltVerifyConstraints is the stored procedure version for the CLI command `dolt constraints verify`.
func doltVerifyConstraints(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltConstraintsVerify(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltConstraintsVerify(ctx *sql.Context, args []string) (int, error) {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return 1, err
	}
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	workingSet, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return 1, err
	}
	workingRoot := workingSet.WorkingRoot()
	headCommit, err := dSess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return 1, err
	}
	h, err := headCommit.HashOf()
	if err != nil {
		return 1, err
	}

	apr, err := cli.CreateVerifyConstraintsArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	verifyAll := apr.Contains(cli.AllFlag)
	outputOnly := apr.Contains(cli.OutputOnlyFlag)

	var comparingRoot *doltdb.RootValue
	if verifyAll {
		comparingRoot, err = doltdb.EmptyRootValue(ctx, workingRoot.VRW(), workingRoot.NodeStore())
		if err != nil {
			return 1, err
		}
	} else {
		comparingRoot, err = headCommit.GetRootValue(ctx)
		if err != nil {
			return 1, err
		}
	}

	tableSet := set.NewStrSet(nil)
	for _, val := range apr.Args {
		_, tableName, ok, err := workingRoot.GetTableInsensitive(ctx, val)
		if err != nil {
			return 1, err
		}
		if !ok {
			return 1, sql.ErrTableNotFound.New(tableName)
		}
		tableSet.Add(tableName)
	}

	newRoot, tablesWithViolations, err := merge.AddForeignKeyViolations(ctx, workingRoot, comparingRoot, tableSet, h)
	if err != nil {
		return 1, err
	}

	if tablesWithViolations.Size() == 0 {
		// no violations were found
		return 0, nil
	}

	// violations were found

	if !outputOnly {
		err = dSess.SetRoot(ctx, dbName, newRoot)
		if err != nil {
			return 1, err
		}
		return 1, nil
	}

	return 1, nil
}
