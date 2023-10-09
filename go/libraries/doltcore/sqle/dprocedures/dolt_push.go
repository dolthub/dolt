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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

var doltPushSchema = []*sql.Column{
	{
		Name:     "status",
		Type:     types.Int64,
		Nullable: false,
	},
	{
		Name:     "return_message",
		Type:     types.LongText,
		Nullable: true,
	},
	{
		Name:     "error_message",
		Type:     types.LongText,
		Nullable: true,
	},
}

// doltPush is the stored procedure version for the CLI command `dolt push`.
func doltPush(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	status, returnMsg, errMsg, err := doDoltPush(ctx, args)
	// err needs to be nil in order to pass the return and error messages to the query caller.
	// otherwise, only err is returned with nil RowIter.
	return rowToIter(int64(status), returnMsg, errMsg), err
}

func doDoltPush(ctx *sql.Context, args []string) (int, string, string, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return returnErr(fmt.Errorf("empty database name"))
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return returnErr(err)
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return returnErr(fmt.Errorf("could not load database %s", dbName))
	}

	apr, err := cli.CreatePushArgParser().Parse(args)
	if err != nil {
		return returnErr(err)
	}

	autoSetUpRemote := loadConfig(ctx).GetStringOrDefault(env.PushAutoSetupRemote, "false")
	pushAutoSetUpRemote, err := strconv.ParseBool(autoSetUpRemote)
	if err != nil {
		return returnErr(err)
	}

	opts, remote, err := env.NewPushOpts(ctx, apr, dbData.Rsr, dbData.Ddb, apr.Contains(cli.ForceFlag), apr.Contains(cli.SetUpstreamFlag), pushAutoSetUpRemote, apr.Contains(cli.AllFlag))
	if err != nil {
		return returnErr(err)
	}
	remoteDB, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), remote, true)
	if err != nil {
		return returnErr(actions.HandleInitRemoteStorageClientErr(remote.Name, remote.Url, err))
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return returnErr(err)
	}

	var msg string
	pushMeta := &env.PushMeta{
		Opts:   opts,
		Remote: remote,
		Rsr:    dbData.Rsr,
		Rsw:    dbData.Rsw,
		SrcDb:  dbData.Ddb,
		DestDb: remoteDB,
		TmpDir: tmpDir,
	}
	msg, err = actions.DoPush(ctx, pushMeta, runProgFuncs, stopProgFuncs)
	if err != nil {
		switch err {
		case doltdb.ErrUpToDate:
			return cmdSuccess, fmt.Sprintf("%s\n%s", msg, "Everything up-to-date"), "", nil
		case datas.ErrMergeNeeded:
			return returnErr(fmt.Errorf("%w; the tip of your current branch is behind its remote counterpart", err))
		default:
			return cmdFailure, msg, err.Error(), nil
		}
	}
	// TODO : set upstream should be persisted outside of session
	return cmdSuccess, msg, "", nil
}

func returnErr(err error) (int, string, string, error) {
	return cmdFailure, "", "", err
}
