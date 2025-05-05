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
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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

	remote, refSpecArgs, err := env.RemoteForFetchArgs(apr.Args, dbData.Rsr)
	if err != nil {
		return cmdFailure, err
	}

	validationErr := validateFetchArgs(apr, refSpecArgs)
	if validationErr != nil {
		return cmdFailure, validationErr
	}

	refSpecs, defaultRefSpec, err := env.ParseRefSpecs(refSpecArgs, dbData.Rsr, remote)
	if err != nil {
		return cmdFailure, err
	}

	if user, hasUser := apr.GetValue(cli.UserFlag); hasUser {
		remote = remote.WithParams(map[string]string{
			dbfactory.GRPCUsernameAuthParam: user,
		})
	}

	srcDB, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), remote, false)
	if err != nil {
		return 1, err
	}

	err = srcDB.Rebase(ctx)
	if err != nil {
		return 1, fmt.Errorf("failed to rebase remote db: %w", err)
	}

	prune := apr.Contains(cli.PruneFlag)
	mode := ref.UpdateMode{Force: true, Prune: prune}
	err = actions.FetchRefSpecs(ctx, dbData, srcDB, refSpecs, defaultRefSpec, &remote, mode, runProgFuncs, stopProgFuncs)
	if err != nil {
		return cmdFailure, fmt.Errorf("fetch failed: %w", err)
	}
	return cmdSuccess, nil
}

// validateFetchArgs returns an error if the arguments provided aren't valid.
func validateFetchArgs(apr *argparser.ArgParseResults, refSpecArgs []string) error {
	if len(refSpecArgs) > 0 && apr.Contains(cli.PruneFlag) {
		// The current prune implementation assumes that we're processing branch specs, which
		return fmt.Errorf("--prune option cannot be provided with a ref spec")
	}

	return nil
}
