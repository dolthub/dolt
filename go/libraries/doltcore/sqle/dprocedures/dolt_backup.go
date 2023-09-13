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
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

const (
	DoltBackupFuncName = "dolt_backup"

	statusOk  = 0
	statusErr = 1
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

	if apr.NArg() == 0 {
		return statusErr, fmt.Errorf("listing existing backup endpoints in sql is not currently implemented. Let us know if you need this by opening a GitHub issue: https://github.com/dolthub/dolt/issues")

	}
	switch apr.Arg(0) {
	case cli.AddBackupId:
		err = addBackup(ctx, dbData, apr)
		if err != nil {
			return statusErr, fmt.Errorf("error adding backup: %w", err)
		}
	case cli.RemoveBackupId, cli.RemoveBackupShortId:
		err = removeBackup(ctx, dbData, apr)
		if err != nil {
			return statusErr, fmt.Errorf("error removing backup: %w", err)
		}
	case cli.RestoreBackupId:
		return statusErr, fmt.Errorf("restoring backup endpoint in sql is unimplemented.")
	case cli.SyncBackupUrlId:
		err = syncBackupViaUrl(ctx, dbData, sess, apr)
		if err != nil {
			return statusErr, fmt.Errorf("error syncing backup url: %w", err)
		}
	case cli.SyncBackupId:
		err = syncBackupViaName(ctx, dbData, sess, apr)
		if err != nil {
			return statusErr, fmt.Errorf("error syncing backup: %w", err)
		}
	default:
		return statusErr, fmt.Errorf("unrecognized dolt_backup parameter: %s", apr.Arg(0))
	}

	return statusOk, nil
}

func addBackup(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 3 {
		return fmt.Errorf("usage: dolt_backup('add', 'backup_name', 'backup-url')")
	}

	backupName := strings.TrimSpace(apr.Arg(1))
	backupUrl := apr.Arg(2)
	cfg := loadConfig(ctx)
	scheme, absBackupUrl, err := env.GetAbsRemoteUrl(filesys.LocalFS, cfg, backupUrl)
	if err != nil {
		return fmt.Errorf("error: '%s' is not valid, %s", backupUrl, err.Error())
	} else if scheme == dbfactory.HTTPScheme || scheme == dbfactory.HTTPSScheme {
		// not sure how to get the dialer so punting on this
		return fmt.Errorf("sync-url does not support http or https backup locations currently")
	}

	params, err := cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
	if err != nil {
		return err
	}

	r := env.NewRemote(backupName, absBackupUrl, params)
	err = dbData.Rsw.AddBackup(r)
	switch err {
	case nil:
		return nil
	case env.ErrBackupAlreadyExists:
		return fmt.Errorf("error: a backup named '%s' already exists, remove it before running this command again", r.Name)
	case env.ErrBackupNotFound:
		return fmt.Errorf("error: unknown backup: '%s' ", r.Name)
	case env.ErrInvalidBackupURL:
		return fmt.Errorf("error: '%s' is not valid, cause: %s", r.Url, err.Error())
	case env.ErrInvalidBackupName:
		return fmt.Errorf("error: invalid backup name: '%s'", r.Name)
	default:
		return fmt.Errorf("error: Unable to save changes, cause: %s", err.Error())
	}
}

func removeBackup(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("usage: dolt_backup('remove', 'backup_name'")
	}

	backupName := strings.TrimSpace(apr.Arg(1))
	err := dbData.Rsw.RemoveBackup(ctx, backupName)
	switch err {
	case nil:
		return nil
	case env.ErrFailedToWriteRepoState:
		return fmt.Errorf("error: failed to save change to repo state, cause: %s", err.Error())
	case env.ErrFailedToDeleteBackup:
		return fmt.Errorf("error: failed to delete backup tracking ref, cause: %s", err.Error())
	case env.ErrFailedToReadFromDb:
		return fmt.Errorf("error: failed to read from db, cause: %s", err.Error())
	case env.ErrBackupNotFound:
		return fmt.Errorf("error: unknown backup: '%s' ", backupName)
	default:
		return fmt.Errorf("error: unknown error, cause: %s", err.Error())
	}
}

func syncBackupViaUrl(ctx *sql.Context, dbData env.DbData, sess *dsess.DoltSession, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("usage: dolt_backup('sync-url', BACKUP_URL)")
	}

	backupUrl := strings.TrimSpace(apr.Arg(1))
	cfg := loadConfig(ctx)
	scheme, absBackupUrl, err := env.GetAbsRemoteUrl(filesys.LocalFS, cfg, backupUrl)
	if err != nil {
		return fmt.Errorf("error: '%s' is not valid.", backupUrl)
	} else if scheme == dbfactory.HTTPScheme || scheme == dbfactory.HTTPSScheme {
		// not sure how to get the dialer so punting on this
		return fmt.Errorf("sync-url does not support http or https backup locations currently")
	}

	params, err := cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
	if err != nil {
		return err
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

	b := env.NewRemote("__temp__", backupUrl, params)

	return syncRoots(ctx, dbData, sess, b)
}

func syncBackupViaName(ctx *sql.Context, dbData env.DbData, sess *dsess.DoltSession, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("usage: dolt_backup('sync', BACKUP_NAME)")
	}

	backupName := strings.TrimSpace(apr.Arg(1))
	backups, err := dbData.Rsr.GetBackups()
	if err != nil {
		return err
	}

	b, ok := backups[backupName]
	if !ok {
		return fmt.Errorf("error: unknown backup: '%s'; %v", backupName, backups)
	}

	return syncRoots(ctx, dbData, sess, b)
}

func syncRoots(ctx *sql.Context, dbData env.DbData, sess *dsess.DoltSession, backup env.Remote) error {
	destDb, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), backup, true)
	if err != nil {
		return fmt.Errorf("error loading backup destination: %w", err)
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return err
	}

	err = actions.SyncRoots(ctx, dbData.Ddb, destDb, tmpDir, runProgFuncs, stopProgFuncs)
	if err != nil && err != pull.ErrDBUpToDate {
		return fmt.Errorf("error syncing backup: %w", err)
	}

	return nil
}
