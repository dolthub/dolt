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
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	DoltBackupProcedureName = "dolt_backup"

	DoltBackupParamAdd     = "add"
	DoltBackupParamRemove  = "remove"
	DoltBackupParamRm      = "rm"
	DoltBackupParamSync    = "sync"
	DoltBackupParamSyncUrl = "sync-url"
	DoltBackupParamRestore = "restore"
)

var awsParamsUsage = []string{
	fmt.Sprintf("--%s=<region>", dbfactory.AWSRegionParam),
	fmt.Sprintf("--%s=<type>", dbfactory.AWSCredsTypeParam),
	fmt.Sprintf("--%s=<file>", dbfactory.AWSCredsFileParam),
	fmt.Sprintf("--%s=<profile>", dbfactory.AWSCredsProfile),
}

// doltBackup implements backup operations for Dolt databases. It routes |args| to the appropriate operation handler
// based on the first argument. The procedure requires superuser privileges and write access to the current database.
// Supported operations are: add, remove/rm, sync, sync-url, and restore.
func doltBackup(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	apr, err := cli.CreateBackupArgParser().Parse(args)
	if err != nil {
		return nil, err
	}

	if apr.NArg() == 0 || (apr.NArg() == 1 && apr.Contains(cli.VerboseFlag)) {
		return nil, fmt.Errorf("use '%s' table to list backups", doltdb.BackupsTableName)
	}

	var dbName string
	funcParam := apr.Arg(0)
	if funcParam != DoltBackupParamRestore {
		dbName = ctx.GetCurrentDatabase()
		if dbName == "" {
			return nil, fmt.Errorf("empty database name")
		}
	}

	err = branch_control.CheckAccess(ctx, branch_control.Permissions_Write)
	if err != nil {
		return nil, err
	}

	if sqlserver.RunningInServerMode() && apr.ContainsAny(cli.AwsParams...) {
		return nil, fmt.Errorf("AWS parameters are unavailable when running in server mode")
	}

	doltSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := doltSess.GetDbData(ctx, dbName)
	if !ok && funcParam != DoltBackupParamRestore {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	switch funcParam {
	case DoltBackupParamAdd:
		if apr.NArg() != 3 {
			return nil, errDoltBackupUsage(funcParam, []string{"name", "url"}, awsParamsUsage)
		}
		err = doltBackupAdd(ctx, dbData, doltSess, apr)
	case DoltBackupParamRemove, DoltBackupParamRm:
		if apr.NArg() != 2 {
			return nil, errDoltBackupUsage(funcParam, []string{"name"}, nil)
		}
		name := apr.Arg(1)
		err = dbData.Rsw.RemoveBackup(ctx, name)
	case DoltBackupParamSync:
		if apr.NArg() != 2 {
			return nil, errDoltBackupUsage(funcParam, []string{"name"}, nil)
		}
		name := apr.Arg(1)
		err = doltBackupSync(ctx, dbData, doltSess, name)
	case DoltBackupParamSyncUrl:
		if apr.NArg() != 2 {
			return nil, errDoltBackupUsage(funcParam, []string{"remote_url"}, awsParamsUsage)
		}
		err = doltBackupSyncUrl(ctx, dbData, doltSess, apr)
	case DoltBackupParamRestore:
		if apr.NArg() != 3 {
			forceParamUsage := []string{fmt.Sprintf("--%s", cli.ForceFlag)}
			return nil, errDoltBackupUsage(funcParam, []string{"remote_url", "new_db_name"}, append(forceParamUsage, awsParamsUsage...))
		}
		err = doltBackupRestore(ctx, dbData, doltSess, apr)
	default:
		return nil, fmt.Errorf("unrecognized %s parameter '%s'", DoltBackupProcedureName, funcParam)
	}

	return rowToIter(int64(0)), err
}

// doltBackupAdd adds a new backup entry with the name and URL specified in |apr|. The URL is normalized to an absolute
// path. AWS parameters are extracted from command-line flags in |apr| if present, otherwise they are loaded from
// session variables if the URL scheme matches.
func doltBackupAdd(ctx *sql.Context, dbData env.DbData[*sql.Context], dsess *dsess.DoltSession, apr *argparser.ArgParseResults) error {
	backupName := apr.Arg(1)
	backupUrlScheme, backupUrl, err := newAbsRemoteUrl(dsess, apr.Arg(2))
	if err != nil {
		return err
	}

	backupParams, err := newParams(apr, backupUrl, backupUrlScheme)
	if err != nil {
		return err
	}

	if len(backupParams) == 0 && backupUrlScheme == dbfactory.AWSScheme {
		backupParams, err = newParamsWithAwsSessionVars(ctx, backupUrlScheme)
		if err != nil {
			return err
		}
	}

	backupRemote := env.NewRemote(backupName, backupUrl, backupParams)
	err = dbData.Rsw.AddBackup(backupRemote)
	return err
}

// doltBackupSync syncs the current database to an existing backup identified by name in |apr|. The backup is looked up
// from the repository state via |dbData.Rsr|. The sync operation copies all roots from the current database to the
// backup location, overwriting any existing data.
func doltBackupSync(ctx *sql.Context, dbData env.DbData[*sql.Context], dsess *dsess.DoltSession, backupName string) error {
	backups, err := dbData.Rsr.GetBackups()
	if err != nil {
		return err
	}

	backupRemote, ok := backups.Get(backupName)
	if !ok {
		return env.ErrBackupNotFound.New(backupName)
	}

	return syncRemote(ctx, dbData, dsess, backupRemote)
}

// doltBackupSyncUrl syncs the current database to a remote URL specified in |apr| without requiring the remote to exist
// in the backups list. The URL is normalized to an absolute path. AWS parameters are extracted from command-line flags
// in |apr| if present, otherwise they are loaded from session variables if the URL scheme matches. The sync operation
// copies all roots from the current database to the remote location, overwriting any existing data.
func doltBackupSyncUrl(ctx *sql.Context, dbData env.DbData[*sql.Context], dsess *dsess.DoltSession, apr *argparser.ArgParseResults) error {
	remoteUrlScheme, remoteUrl, err := newAbsRemoteUrl(dsess, apr.Arg(1))
	if err != nil {
		return err
	}

	remoteParams, err := newParams(apr, remoteUrl, remoteUrlScheme)
	if err != nil {
		return err
	}

	if len(remoteParams) == 0 && remoteUrlScheme == dbfactory.AWSScheme {
		remoteParams, err = newParamsWithAwsSessionVars(ctx, remoteUrlScheme)
		if err != nil {
			return err
		}
	}

	remote := env.NewRemote(DoltBackupParamSyncUrl, remoteUrl, remoteParams)
	return syncRemote(ctx, dbData, dsess, remote)
}

// doltBackupRestore clones a database from the remote URL specified in |apr| into a new database with the name
// specified. The URL is normalized to an absolute path. AWS parameters are extracted from command-line flags in |apr|
// if present. If no command-line parameters are provided, AWS parameters are loaded from session variables if the URL
// scheme matches.
//
// If the target database already exists, the restore operation fails unless the --force flag is provided, in which case
// the existing database is dropped before cloning.
func doltBackupRestore(ctx *sql.Context, dbData env.DbData[*sql.Context], dsess *dsess.DoltSession, apr *argparser.ArgParseResults) error {
	remoteUrlScheme, remoteUrl, err := newAbsRemoteUrl(dsess, apr.Arg(1))
	if err != nil {
		return err
	}

	remoteParams, err := newParams(apr, remoteUrl, remoteUrlScheme)
	if err != nil {
		return err
	}

	if len(remoteParams) == 0 && remoteUrlScheme == dbfactory.AWSScheme {
		remoteParams, err = newParamsWithAwsSessionVars(ctx, remoteUrlScheme)
		if err != nil {
			return err
		}
	}

	remote := env.NewRemote(DoltBackupParamRestore, remoteUrl, remoteParams)

	// Use default format if no database context is available (e.g., when run from invalid directory).
	format := types.Format_Default
	if dbData.Ddb != nil {
		format = dbData.Ddb.Format()
	}

	remoteDb, err := dsess.Provider().GetRemoteDB(ctx, format, remote, true)
	if err != nil {
		return err
	}

	lookupDbName := apr.Arg(2)
	hasLookupDb := dsess.Provider().HasDatabase(ctx, lookupDbName)
	// We can't only check the databases from memory since this command can be run from subdirectories.
	fileSys := dsess.GetFileSystem()
	lookupDbInFileSys, _ := fileSys.Exists(lookupDbName)
	forceRestore := apr.Contains(cli.ForceFlag)
	if (hasLookupDb || lookupDbInFileSys) && !forceRestore {
		return fmt.Errorf("database '%s' already exists, use '--%s' to overwrite", lookupDbName, cli.ForceFlag)
	}

	if hasLookupDb {
		err = dsess.Provider().DropDatabase(ctx, lookupDbName)
		if err != nil {
			return err
		}
	}

	if lookupDbInFileSys && !hasLookupDb {
		err = fileSys.Delete(lookupDbName, forceRestore)
		if err != nil {
			return err
		}
	}

	err = dsess.Provider().CreateDatabase(ctx, lookupDbName)
	if err != nil {
		return err
	}

	newDb, _, err := dsess.Provider().SessionDatabase(ctx, lookupDbName)
	if err != nil {
		return err
	}

	// Unlike CloneDatabaseFromRemote which clones tracking branches (remote refs), we need all local changes.
	return actions.SyncRoots(ctx, remoteDb, newDb.DbData().Ddb, fileSys.TempDir(), runProgFuncs, stopProgFuncs)
}

// syncRemote syncs the roots from |dbData| to the remote specified by |remote|. It prepares the remote database
// location using PrepareDB, which creates directories for file:// URLs if they do not exist. The sync operation copies
// all chunks from the source database to the destination, effectively overwriting the destination to match the source.
func syncRemote(ctx *sql.Context, dbData env.DbData[*sql.Context], dsess *dsess.DoltSession, remote env.Remote) error {
	// Commit the current session's working set to the persistent chunk store. This ensures that uncommitted transaction
	// changes (e.g. INSERTs) are usually visible to the backup procedure, which reads directly from the roots.
	err := dsess.CommitWorkingSet(ctx, ctx.GetCurrentDatabase(), ctx.GetTransaction())
	if err != nil {
		return err
	}

	params := map[string]interface{}{}
	for k, v := range remote.Params {
		params[k] = v
	}

	// This fails with unsupported schemes (i.e. http[s]), but in such cases we shouldn't have to prepare the database.
	// We primarily use this to initialize the directory for file URLs without a directory.
	_ = dbfactory.PrepareDB(ctx, dbData.Ddb.Format(), remote.Url, params)
	destDb, err := dsess.Provider().GetRemoteDB(ctx, dbData.Ddb.Format(), remote, true)
	if err != nil {
		return err
	}

	err = actions.SyncRoots(ctx, dbData.Ddb, destDb, dsess.GetFileSystem().TempDir(), runProgFuncs, stopProgFuncs)
	if err != nil && !errors.Is(err, pull.ErrDBUpToDate) {
		return err
	}

	return nil
}

// newParams extracts AWS-specific parameters from command-line flags in |apr| if |urlScheme| is AWS. If the scheme is
// not AWS, it verifies that no AWS parameters are present in |apr|.
func newParams(apr *argparser.ArgParseResults, url string, urlScheme string) (map[string]string, error) {
	params := map[string]string{}
	var err error
	switch urlScheme {
	case dbfactory.AWSScheme:
		err = cli.AddAWSParams(url, apr, params)
	case dbfactory.OSSScheme:
		// TODO(elianddb): This func mainly interfaces with apr to set the OSS key-vals in params, but the backup arg
		//  parser does not include any OSS-related flags? I'm guessing they must be processed elsewhere?
		err = cli.AddOSSParams(url, apr, params)
	case dbfactory.GitFileScheme, dbfactory.GitHTTPScheme, dbfactory.GitHTTPSScheme, dbfactory.GitSSHScheme:
		err = cli.VerifyNoAwsParams(apr)
		if dir, ok := apr.GetValue("git-cache-dir"); ok {
			dir = strings.TrimSpace(dir)
			if dir != "" {
				params[dbfactory.GitCacheDirParam] = dir
			}
		}
	default:
		err = cli.VerifyNoAwsParams(apr)
	}
	return params, err
}

// newParamsWithAwsSessionVars extracts AWS-specific parameters from read-only session variables in |ctx|. It reads
// aws_credentials_file, aws_credentials_profile, and aws_credentials_region session variables and builds a parameter
// map. If URL scheme is not AWS, an empty parameter map is returned.
func newParamsWithAwsSessionVars(ctx *sql.Context, urlScheme string) (map[string]string, error) {
	params := map[string]string{}

	credsFile, err := ctx.Session.GetSessionVariable(ctx, dsess.AwsCredsFile)
	if err != nil {
		return nil, err
	}
	credsFileStr, isStr := credsFile.(string)
	if isStr && len(credsFileStr) > 0 {
		params[dbfactory.AWSCredsFileParam] = credsFileStr
	}

	credsProfile, err := ctx.Session.GetSessionVariable(ctx, dsess.AwsCredsProfile)
	if err != nil {
		return nil, err
	}
	profStr, isStr := credsProfile.(string)
	if isStr && len(profStr) > 0 {
		params[dbfactory.AWSCredsProfile] = profStr
	}

	credsRegion, err := ctx.Session.GetSessionVariable(ctx, dsess.AwsCredsRegion)
	if err != nil {
		return nil, err
	}
	regionStr, isStr := credsRegion.(string)
	if isStr && len(regionStr) > 0 {
		params[dbfactory.AWSRegionParam] = regionStr
	}

	return params, nil
}

// newAbsRemoteUrl normalizes the |url| to an absolute path and returns the URL scheme and the normalized URL. It loads
// the Dolt CLI configuration from the filesystem accessible via |dsess| and uses GetAbsRemoteUrl to perform the
// normalization. HTTPS URLs without an explicit scheme default to the configured remotes API host.
func newAbsRemoteUrl(dsess *dsess.DoltSession, url string) (string, string, error) {
	if url == "" {
		return "", "", env.ErrBackupInvalidUrl.New(url)
	}
	config, err := env.LoadDoltCliConfig(env.GetCurrentUserHomeDir, dsess.GetFileSystem())
	if err != nil {
		return "", "", err
	}
	return env.GetAbsRemoteUrl(dsess.GetFileSystem(), config, url)
}

// errDoltBackupUsage constructs a usage error message for the dolt_backup procedure. It formats |funcParam| as the
// operation, |requiredParams| as required positional arguments, and |optionalParams| as optional flag arguments. The
// resulting error message follows the format:
// "usage: dolt_backup('<param>', '<required1>', ..., ['<optional1>'], ...)".
func errDoltBackupUsage(funcParam string, requiredParams, optionalParams []string) error {
	var builder strings.Builder

	builder.WriteString("usage: ")
	builder.WriteString(DoltBackupProcedureName)
	builder.WriteString("('")
	builder.WriteString(funcParam)
	builder.WriteByte('\'')

	for _, req := range requiredParams {
		builder.WriteString(", '")
		builder.WriteString(req)
		builder.WriteByte('\'')
	}

	for _, opt := range optionalParams {
		builder.WriteString(", ['")
		builder.WriteString(opt)
		builder.WriteString("']")
	}

	builder.WriteByte(')')

	return errors.New(builder.String())
}
