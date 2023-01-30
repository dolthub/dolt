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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

const (
	DoltBackupFuncName = "dolt_backup"

	statusOk  = 1
	statusErr = 0
)

// doltBackup is the stored procedure version for the CLI command `dolt backup`.
func doltBackup(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltBackup(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltBackup(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()
	if len(dbName) == 0 {
		return statusErr, fmt.Errorf("Empty database name.")
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err != nil {
		return statusErr, err
	}

	apr, err := cli.CreateBackupArgParser().Parse(args)
	if err != nil {
		return statusErr, err
	}

	invalidParams := []string{dbfactory.AWSCredsFileParam, dbfactory.AWSCredsProfile, dbfactory.AWSCredsTypeParam, dbfactory.AWSRegionParam}
	for _, param := range invalidParams {
		if apr.Contains(param) {
			return statusErr, fmt.Errorf("parameter '%s' is not supported when running this command via SQL", param)
		}
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return statusErr, sql.ErrDatabaseNotFound.New(dbName)
	}

	var b env.Remote
	switch {
	case apr.NArg() == 0:
		return statusErr, fmt.Errorf("listing existing backups endpoints in sql is unimplemented.")
	case apr.Arg(0) == cli.AddBackupId:
		return statusErr, fmt.Errorf("adding backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.RemoveBackupId:
		return statusErr, fmt.Errorf("removing backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.RemoveBackupShortId:
		return statusErr, fmt.Errorf("removing backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.RestoreBackupId:
		return statusErr, fmt.Errorf("restoring backup endpoint in sql is unimplemented.")
	case apr.Arg(0) == cli.SyncBackupUrlId:
		if apr.NArg() != 2 {
			return statusErr, fmt.Errorf("usage: dolt_backup('sync-url', BACKUP_URL)")
		}

		backupUrl := strings.TrimSpace(apr.Arg(1))
		cfg := loadConfig(ctx)
		scheme, absBackupUrl, err := env.GetAbsRemoteUrl(filesys.LocalFS, cfg, backupUrl)
		if err != nil {
			return statusErr, fmt.Errorf("error: '%s' is not valid.", backupUrl)
		} else if scheme == dbfactory.HTTPScheme || scheme == dbfactory.HTTPSScheme {
			// not sure how to get the dialer so punting on this
			return statusErr, fmt.Errorf("sync-url does not support http or https backup locations currently")
		}

		params, err := cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
		if err != nil {
			return statusErr, err
		}

		credsFile, _ := sess.GetSessionVariable(ctx, dsess.AwsCredsFile)
		credsFileStr, isStr := credsFile.(string)
		if isStr && len(credsFileStr) > 0 {
			params[dbfactory.AWSCredsFileParam] = credsFileStr
		}

		credsProfile, err := sess.GetSessionVariable(ctx, dsess.AwsCredsProfile)
		profStr, isStr := credsProfile.(string)
		if isStr && len(profStr) > 0 {
			params[dbfactory.AWSCredsProfile] = profStr
		}

		credsRegion, err := sess.GetSessionVariable(ctx, dsess.AwsCredsRegion)
		regionStr, isStr := credsRegion.(string)
		if isStr && len(regionStr) > 0 {
			params[dbfactory.AWSRegionParam] = regionStr
		}

		b = env.NewRemote("__temp__", backupUrl, params)

	case apr.Arg(0) == cli.SyncBackupId:
		if apr.NArg() != 2 {
			return statusErr, fmt.Errorf("usage: dolt_backup('sync', BACKUP_NAME)")
		}

		backupName := strings.TrimSpace(apr.Arg(1))

		backups, err := dbData.Rsr.GetBackups()
		if err != nil {
			return statusErr, err
		}

		b, ok = backups[backupName]
		if !ok {
			return statusErr, fmt.Errorf("error: unknown backup: '%s'; %v", backupName, backups)
		}

	default:
		return statusErr, fmt.Errorf("unrecognized dolt_backup parameter: %s", apr.Arg(0))
	}

	destDb, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), b, true)
	if err != nil {
		return statusErr, fmt.Errorf("error loading backup destination: %w", err)
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return statusErr, err
	}
	err = actions.SyncRoots(ctx, dbData.Ddb, destDb, tmpDir, runProgFuncs, stopProgFuncs)
	if err != nil && err != pull.ErrDBUpToDate {
		return 1, fmt.Errorf("error syncing backup: %w", err)
	}
	return statusOk, nil
}
