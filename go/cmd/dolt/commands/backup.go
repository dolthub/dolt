// Copyright 2021 Dolthub, Inc.
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

package commands

import (
	"context"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var backupDocs = cli.CommandDocumentationContent{
	ShortDesc: "Manage server backups",
	LongDesc: `With no arguments, shows a list of existing backups. Several subcommands are available to perform operations on backups, point in time snapshots of a database's contents.

{{.EmphasisLeft}}add{{.EmphasisRight}}
Adds a backup named {{.LessThan}}name{{.GreaterThan}} for the database at {{.LessThan}}url{{.GreaterThan}}.
The {{.LessThan}}url{{.GreaterThan}} parameter supports url schemes of http, https, aws, gs, and file. The url prefix defaults to https. If the {{.LessThan}}url{{.GreaterThan}} parameter is in the format {{.EmphasisLeft}}<organization>/<repository>{{.EmphasisRight}} then dolt will use the {{.EmphasisLeft}}backups.default_host{{.EmphasisRight}} from your configuration file (Which will be dolthub.com unless changed).
The URL address must be unique to existing remotes and backups.

AWS cloud backup urls should be of the form {{.EmphasisLeft}}aws://[dynamo-table:s3-bucket]/database{{.EmphasisRight}}. You may configure your aws cloud backup using the optional parameters {{.EmphasisLeft}}aws-region{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-type{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-file{{.EmphasisRight}}.

aws-creds-type specifies the means by which credentials should be retrieved in order to access the specified cloud resources (specifically the dynamo table, and the s3 bucket). Valid values are 'role', 'env', or 'file'.

	role: Use the credentials installed for the current user
	env: Looks for environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	file: Uses the credentials file specified by the parameter aws-creds-file

	
GCP backup urls should be of the form gs://gcs-bucket/database and will use the credentials setup using the gcloud command line available from Google.

The local filesystem can be used as a backup by providing a repository url in the format file://absolute path. See https://en.wikipedia.org/wiki/File_URI_scheme

{{.EmphasisLeft}}remove{{.EmphasisRight}}, {{.EmphasisLeft}}rm{{.EmphasisRight}}
Remove the backup named {{.LessThan}}name{{.GreaterThan}}. All configuration settings for the backup are removed. The contents of the backup are not affected.

{{.EmphasisLeft}}restore{{.EmphasisRight}}
Restore a Dolt database from a given {{.LessThan}}url{{.GreaterThan}} into a specified directory {{.LessThan}}name{{.GreaterThan}}. This will fail if {{.LessThan}}name{{.GreaterThan}} is already a Dolt database unless '--force' is provided, in which case the existing database will be overwritten with the contents of the restored backup.

{{.EmphasisLeft}}sync{{.EmphasisRight}}
Snapshot the database and upload to the backup {{.LessThan}}name{{.GreaterThan}}. This includes branches, tags, working sets, and remote tracking refs.

	
{{.EmphasisLeft}}sync-url{{.EmphasisRight}}
Snapshot the database and upload the backup to {{.LessThan}}url{{.GreaterThan}}. Like sync, this includes branches, tags, working sets, and remote tracking refs, but it does not require you to create a named backup`,

	Synopsis: []string{
		"[-v | --verbose]",
		"add [--aws-region {{.LessThan}}region{{.GreaterThan}}] [--aws-creds-type {{.LessThan}}creds-type{{.GreaterThan}}] [--aws-creds-file {{.LessThan}}file{{.GreaterThan}}] [--aws-creds-profile {{.LessThan}}profile{{.GreaterThan}}] {{.LessThan}}name{{.GreaterThan}} {{.LessThan}}url{{.GreaterThan}}",
		"remove {{.LessThan}}name{{.GreaterThan}}",
		"restore [--force] {{.LessThan}}url{{.GreaterThan}} {{.LessThan}}name{{.GreaterThan}}",
		"sync {{.LessThan}}name{{.GreaterThan}}",
		"sync-url [--aws-region {{.LessThan}}region{{.GreaterThan}}] [--aws-creds-type {{.LessThan}}creds-type{{.GreaterThan}}] [--aws-creds-file {{.LessThan}}file{{.GreaterThan}}] [--aws-creds-profile {{.LessThan}}profile{{.GreaterThan}}] {{.LessThan}}url{{.GreaterThan}}",
	},
}

type BackupCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd BackupCmd) Name() string {
	return "backup"
}

// Description returns a description of the command
func (cmd BackupCmd) Description() string {
	return "Manage a set of server backups."
}

func (cmd BackupCmd) RequiresRepo() bool {
	return false
}

func (cmd BackupCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(backupDocs, ap)
}

func (cmd BackupCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateBackupArgParser()
}

// EventType returns the type of the event to log
func (cmd BackupCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_REMOTE
}

// Exec executes the command
func (cmd BackupCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, backupDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	var verr errhand.VerboseError

	// All the sub commands except `restore` require a valid environment
	if apr.NArg() == 0 || apr.Arg(0) != cli.RestoreBackupId {
		if !cli.CheckEnvIsValid(dEnv) {
			return 2
		}
	}

	switch {
	case apr.NArg() == 0:
		verr = printBackups(queryist.Context, queryist.Queryist, apr)
	case apr.Arg(0) == cli.AddBackupId:
		verr = addBackup(queryist.Context, queryist.Queryist, dEnv, apr)
	case apr.Arg(0) == cli.RemoveBackupId:
		verr = removeBackup(queryist.Context, queryist.Queryist, apr)
	case apr.Arg(0) == cli.RemoveBackupShortId:
		verr = removeBackup(queryist.Context, queryist.Queryist, apr)
	case apr.Arg(0) == cli.SyncBackupId:
		verr = syncBackup(queryist.Context, queryist.Queryist, apr)
	case apr.Arg(0) == cli.SyncBackupUrlId:
		verr = syncBackupUrl(queryist.Context, queryist.Queryist, dEnv, apr)
	case apr.Arg(0) == cli.RestoreBackupId:
		verr = restoreBackup(queryist.Context, queryist.Queryist, dEnv, apr)
	default:
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func removeBackup(sqlCtx *sql.Context, queryist cli.Queryist, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	backupName := strings.TrimSpace(apr.Arg(1))
	qry, err := dbr.InterpolateForDialect("CALL dolt_backup('remove', ?)", []interface{}{backupName}, dialect.MySQL)
	if err != nil {
		return errhand.BuildDError("error: failed to build query").AddCause(err).Build()
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return errhand.BuildDError("error: failed to delete backup tracking ref").AddCause(err).Build()
	}

	return nil
}

func addBackup(sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	backupName := strings.TrimSpace(apr.Arg(1))

	backupUrl := apr.Arg(2)
	scheme, absBackupUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, backupUrl)
	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", backupUrl).AddCause(err).Build()
	}

	_, err = cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	qry, err := dbr.InterpolateForDialect("CALL dolt_backup('add', ?, ?)", []interface{}{backupName, absBackupUrl}, dialect.MySQL)
	if err != nil {
		return errhand.BuildDError("error: failed to build query").AddCause(err).Build()
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return errhand.BuildDError("error: failed to add backup").AddCause(err).Build()
	}

	return nil
}

func printBackups(sqlCtx *sql.Context, queryist cli.Queryist, apr *argparser.ArgParseResults) errhand.VerboseError {
	var qry string
	if apr.Contains(cli.VerboseFlag) {
		qry = "SELECT * FROM dolt_backups('--verbose')"
	} else {
		qry = "SELECT * FROM dolt_backups()"
	}

	rows, err := cli.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return errhand.BuildDError("Unable to get backups from the local directory").AddCause(err).Build()
	}

	for _, row := range rows {
		name, ok := row[0].(string)
		if !ok {
			return errhand.BuildDError("unexpectedly received non-string name column from dolt_backups: %v", row[0]).Build()
		}

		if apr.Contains(cli.VerboseFlag) {
			if len(row) < 3 {
				return errhand.BuildDError("unexpectedly received insufficient columns from dolt_backups: expected 3, got %d", len(row)).Build()
			}
			url, ok := row[1].(string)
			if !ok {
				return errhand.BuildDError("unexpectedly received non-string url column from dolt_backups: %v", row[1]).Build()
			}
			paramStr, err := getJsonAsString(sqlCtx, row[2])
			if err != nil {
				return errhand.BuildDError("unexpectedly received invalid params column from dolt_backups").AddCause(err).Build()
			}
			cli.Printf("%s %s %s\n", name, url, paramStr)
		} else {
			cli.Println(name)
		}
	}

	return nil
}

func syncBackupUrl(sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	backupUrl := apr.Arg(1)
	scheme, absBackupUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, backupUrl)
	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", backupUrl).AddCause(err).Build()
	}

	_, err = cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	qry, err := dbr.InterpolateForDialect("CALL dolt_backup('sync-url', ?)", []interface{}{absBackupUrl}, dialect.MySQL)
	if err != nil {
		return errhand.BuildDError("error: failed to build query").AddCause(err).Build()
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return errhand.BuildDError("error: failed to sync backup").AddCause(err).Build()
	}

	return nil
}

func syncBackup(sqlCtx *sql.Context, queryist cli.Queryist, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	backupName := strings.TrimSpace(apr.Arg(1))
	qry, err := dbr.InterpolateForDialect("CALL dolt_backup('sync', ?)", []interface{}{backupName}, dialect.MySQL)
	if err != nil {
		return errhand.BuildDError("error: failed to build query").AddCause(err).Build()
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return errhand.BuildDError("error: failed to sync backup").AddCause(err).Build()
	}

	return nil
}

func restoreBackup(sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() < 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}
	apr.Args = apr.Args[1:]
	restoredDB, urlStr, verr := parseArgs(apr)
	if verr != nil {
		return verr
	}

	_, remoteUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, urlStr)
	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", urlStr).Build()
	}

	if err := cli.VerifyNoAwsParams(apr); err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	procArgs := []string{"restore", remoteUrl, restoredDB}
	if apr.Contains(cli.ForceFlag) {
		procArgs = []string{"restore", "--force", remoteUrl, restoredDB}
	}
	qry, err := interpolateStoredProcedureCall("dolt_backup", procArgs)
	if err != nil {
		return errhand.BuildDError("error: failed to build query").AddCause(err).Build()
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return errhand.BuildDError("error: failed to restore backup").AddCause(err).Build()
	}

	return nil
}
