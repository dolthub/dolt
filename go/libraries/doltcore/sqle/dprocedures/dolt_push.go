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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

// doltPush is the stored procedure version for the CLI command `dolt push`.
func doltPush(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltPush(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltPush(ctx *sql.Context, args []string) (int, error) {
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
		return cmdFailure, fmt.Errorf("could not load database %s", dbName)
	}

	apr, err := cli.CreatePushArgParser().Parse(args)
	if err != nil {
		return cmdFailure, err
	}

	autoSetUpRemote := loadConfig(ctx).GetStringOrDefault(env.PushAutoSetupRemote, "false")
	pushAutoSetUpRemote, err := strconv.ParseBool(autoSetUpRemote)
	if err != nil {
		return cmdFailure, err
	}

	opts, err := env.NewPushOpts(ctx, apr, dbData.Rsr, dbData.Ddb, apr.Contains(cli.ForceFlag), apr.Contains(cli.SetUpstreamFlag), pushAutoSetUpRemote)
	if err != nil {
		return cmdFailure, err
	}
	remoteDB, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), opts.Remote, true)
	if err != nil {
		return 1, actions.HandleInitRemoteStorageClientErr(opts.Remote.Name, opts.Remote.Url, err)
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return cmdFailure, err
	}
	err = actions.DoPush(ctx, dbData.Rsr, dbData.Rsw, dbData.Ddb, remoteDB, tmpDir, opts, runProgFuncs, stopProgFuncs)
	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return cmdSuccess, nil
		case datas.ErrMergeNeeded:
			return cmdFailure, fmt.Errorf("%w; the tip of your current branch is behind its remote counterpart", err)
		default:
			return cmdFailure, err
		}
	}
	// TODO : set upstream should be persisted outside of session
	return cmdSuccess, nil
}
