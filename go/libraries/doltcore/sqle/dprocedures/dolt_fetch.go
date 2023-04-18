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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// doltFetch is the stored procedure version for the CLI command `dolt fetch`.
func doltFetch(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltFetch(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltFetch(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return cmdFailure, fmt.Errorf("empty database name")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return cmdFailure, err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return cmdFailure, fmt.Errorf("Could not load database %s", dbName)
	}

	apr, err := cli.CreateFetchArgParser().Parse(args)
	if err != nil {
		return cmdFailure, err
	}

	remote, refSpecs, err := env.NewFetchOpts(apr.Args, dbData.Rsr)
	if err != nil {
		return cmdFailure, err
	}

	srcDB, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), remote, false)
	if err != nil {
		return 1, err
	}

	err = actions.FetchRefSpecs(ctx, dbData, srcDB, refSpecs, remote, ref.UpdateMode{Force: true}, runProgFuncs, stopProgFuncs)
	if err != nil {
		return cmdFailure, fmt.Errorf("fetch failed: %w", err)
	}
	return cmdSuccess, nil
}
