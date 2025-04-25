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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/datas"
)

var doltPushSchema = []*sql.Column{
	{
		Name:     "status",
		Type:     types.Int64,
		Nullable: false,
	},
	{
		Name:     "message",
		Type:     types.LongText,
		Nullable: true,
	},
}

// doltPush is the stored procedure version for the CLI command `dolt push`.
func doltPush(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, message, err := doDoltPush(ctx, args)
	return rowToIter(int64(res), message), err
}

func doDoltPush(ctx *sql.Context, args []string) (int, string, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return cmdFailure, "", fmt.Errorf("empty database name")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return cmdFailure, "", err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return cmdFailure, "", fmt.Errorf("could not load database %s", dbName)
	}

	apr, err := cli.CreatePushArgParser().Parse(args)
	if err != nil {
		return cmdFailure, "", err
	}

	autoSetUpRemote := loadConfig(ctx).GetStringOrDefault(config.PushAutoSetupRemote, "false")
	pushAutoSetUpRemote, err := strconv.ParseBool(autoSetUpRemote)
	if err != nil {
		return cmdFailure, "", err
	}

	targets, remote, err := env.NewPushOpts(ctx, apr, dbData.Rsr, dbData.Ddb, apr.Contains(cli.ForceFlag), apr.Contains(cli.SetUpstreamFlag), pushAutoSetUpRemote, apr.Contains(cli.AllFlag))
	if err != nil {
		return cmdFailure, "", err
	}

	if user, hasUser := apr.GetValue(cli.UserFlag); hasUser {
		rmt := (*remote).WithParams(map[string]string{
			dbfactory.GRPCUsernameAuthParam: user,
		})
		remote = &rmt
	}

	remoteDB, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), *remote, true)
	if err != nil {
		return cmdFailure, "", actions.HandleInitRemoteStorageClientErr(remote.Name, remote.Url, err)
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return cmdFailure, "", err
	}

	var returnMsg string
	po := &env.PushOptions[*sql.Context]{
		Targets: targets,
		Remote:  remote,
		Rsr:     dbData.Rsr,
		Rsw:     dbData.Rsw,
		SrcDb:   dbData.Ddb,
		DestDb:  remoteDB,
		TmpDir:  tmpDir,
	}
	returnMsg, err = actions.DoPush(ctx, po, runProgFuncs, stopProgFuncs)
	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return cmdSuccess, "Everything up-to-date", nil
		case datas.ErrMergeNeeded:
			return cmdFailure, returnMsg, fmt.Errorf("%w; the tip of your current branch is behind its remote counterpart", err)
		default:
			if returnMsg != "" {
				// For multiple branches push, we need to print successful push message
				// before the error message. We currently cannot return success message
				// if there was a failed push with error. So, we need to include the success
				// message in the error message before returning.
				err = fmt.Errorf("%s\n%s", returnMsg, err.Error())
			}
			return cmdFailure, "", err
		}
	}
	// TODO : set upstream should be persisted outside of session
	return cmdSuccess, returnMsg, nil
}
