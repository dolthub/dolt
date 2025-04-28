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
	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/types"
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
		if err = restoreBackup(ctx, dbData, apr); err != nil {
			return statusErr, fmt.Errorf("error restoring backup: %w", err)
		}
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

func addBackup(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults) error {
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

func restoreBackup(ctx *sql.Context, _ env.DbData[*sql.Context], apr *argparser.ArgParseResults) error {
	if apr.NArg() != 3 {
		return fmt.Errorf("usage: dolt_backup('restore', 'backup_url', 'database_name')")
	}

	// Only allow admins to restore a database
	if err := checkBackupRestorePrivs(ctx); err != nil {
		return err
	}

	backupUrl := strings.TrimSpace(apr.Arg(1))
	dbName := strings.TrimSpace(apr.Arg(2))
	force := apr.Contains(cli.ForceFlag)

	sess := dsess.DSessFromSess(ctx.Session)

	params, err := loadAwsParams(ctx, sess, apr, backupUrl, "restore")
	if err != nil {
		return err
	}

	r := env.NewRemote("", backupUrl, params)
	srcDb, err := r.GetRemoteDB(ctx, types.Format_Default, nil)
	if err != nil {
		return err
	}

	existingDbData, restoringExistingDb := sess.GetDbData(ctx, dbName)
	if restoringExistingDb {
		if !force {
			return fmt.Errorf("error: cannot restore backup into %s. "+
				"A database with that name already exists. Did you mean to supply --force?", dbName)
		}

		return syncRootsFromBackup(ctx, existingDbData, sess, r)
	} else {
		// Track whether the db directory existed before we tried to create it, so we can clean up on errors
		userDirExisted, _ := sess.Provider().FileSystem().Exists(dbName)

		// Create a new Dolt env for the clone; use env.NoRemote to avoid origin upstream
		clonedEnv, err := actions.EnvForClone(ctx, srcDb.ValueReadWriter().Format(), env.NoRemote, dbName,
			sess.Provider().FileSystem(), doltversion.Version, env.GetCurrentUserHomeDir)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		// make empty repo state
		_, err = env.CreateRepoState(clonedEnv.FS, env.DefaultInitBranch)
		if err != nil {
			return err
		}

		if err = syncRootsFromBackup(ctx, clonedEnv.DbData(ctx), sess, r); err != nil {
			// If we're cloning into a directory that already exists do not erase it.
			// Otherwise, make a best effort to delete any directory we created.
			if userDirExisted {
				_ = clonedEnv.FS.Delete(dbfactory.DoltDir, true)
			} else {
				_ = clonedEnv.FS.Delete(".", true)
			}
		}
		return err
	}
}

func removeBackup(ctx *sql.Context, dbData env.DbData[*sql.Context], apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("usage: dolt_backup('remove', 'backup_name')")
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

func loadAwsParams(ctx *sql.Context, sess *dsess.DoltSession, apr *argparser.ArgParseResults, backupUrl, backupCmd string) (map[string]string, error) {
	cfg := loadConfig(ctx)
	scheme, absBackupUrl, err := env.GetAbsRemoteUrl(filesys.LocalFS, cfg, backupUrl)
	if err != nil {
		return nil, fmt.Errorf("error: '%s' is not valid.", backupUrl)
	} else if scheme == dbfactory.HTTPScheme || scheme == dbfactory.HTTPSScheme {
		// not sure how to get the dialer so punting on this
		return nil, fmt.Errorf("%s does not support http or https backup locations currently", backupCmd)
	}

	params, err := cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
	if err != nil {
		return nil, err
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

	return params, nil
}

func syncBackupViaUrl(ctx *sql.Context, dbData env.DbData[*sql.Context], sess *dsess.DoltSession, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("usage: dolt_backup('sync-url', BACKUP_URL)")
	}

	backupUrl := strings.TrimSpace(apr.Arg(1))
	params, err := loadAwsParams(ctx, sess, apr, backupUrl, "sync-url")
	if err != nil {
		return err
	}

	b := env.NewRemote("__temp__", backupUrl, params)

	return syncRootsToBackup(ctx, dbData, sess, b)
}

func syncBackupViaName(ctx *sql.Context, dbData env.DbData[*sql.Context], sess *dsess.DoltSession, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return fmt.Errorf("usage: dolt_backup('sync', BACKUP_NAME)")
	}

	backupName := strings.TrimSpace(apr.Arg(1))
	backups, err := dbData.Rsr.GetBackups()
	if err != nil {
		return err
	}

	b, ok := backups.Get(backupName)
	if !ok {
		return fmt.Errorf("error: unknown backup: '%s'; %v", backupName, backups)
	}

	return syncRootsToBackup(ctx, dbData, sess, b)
}

// syncRootsToBackup syncs the roots from |dbData| to the backup specified by |backup|.
func syncRootsToBackup(ctx *sql.Context, dbData env.DbData[*sql.Context], sess *dsess.DoltSession, backup env.Remote) error {
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

// syncRootsFromBackup syncs the roots from the backup specified by |backup| to |dbData|.
func syncRootsFromBackup[C doltdb.Context](ctx *sql.Context, dbData env.DbData[C], sess *dsess.DoltSession, backup env.Remote) error {
	destDb, err := sess.Provider().GetRemoteDB(ctx, dbData.Ddb.ValueReadWriter().Format(), backup, true)
	if err != nil {
		return fmt.Errorf("error loading backup destination: %w", err)
	}

	tmpDir, err := dbData.Rsw.TempTableFilesDir()
	if err != nil {
		return err
	}

	err = actions.SyncRoots(ctx, destDb, dbData.Ddb, tmpDir, runProgFuncs, stopProgFuncs)
	if err != nil && err != pull.ErrDBUpToDate {
		return fmt.Errorf("error syncing backup: %w", err)
	}

	return nil
}

// UserHasSuperAccess returns whether the current user has SUPER access. This is used by
// Doltgres to check the user role by its own authentication methods.
var UserHasSuperAccess = userHasSuperAccess

func userHasSuperAccess(ctx *sql.Context) (bool, error) {
	privs, counter := ctx.GetPrivilegeSet()
	if counter == 0 {
		return false, fmt.Errorf("unable to check user privileges")
	}
	return privs.Has(sql.PrivilegeType_Super) == true, nil
}

// checkBackupRestorePrivs returns an error if the user requesting to restore a database
// does not have SUPER access. Since this is a potentially destructive operation, we restrict it to admins,
// even though the SUPER privilege has been deprecated, since there isn't another appropriate global privilege.
func checkBackupRestorePrivs(ctx *sql.Context) error {
	isSuper, err := UserHasSuperAccess(ctx)
	if err != nil {
		return fmt.Errorf("error in dolt_backup() restore subcommand: %w", err)
	}
	if !isSuper {
		return sql.ErrPrivilegeCheckFailed.New(ctx.Session.Client().User)
	}

	return nil
}
