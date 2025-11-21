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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
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

var VerboseErrorUsage = errhand.BuildDError("").SetPrintUsage().Build()

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

// Exec executes the `dolt backup` command with the provided subcommand. If no subcommand is provided, the dolt_backups
// table will be printed.
func (cmd BackupCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	argParser := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, backupDocs, argParser))
	apr := cli.ParseArgsOrDie(argParser, args, help)

	queryEngine, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if apr.NArg() == 0 {
		verboseErr := printDoltBackupsTable(&queryEngine, apr)
		return HandleVErrAndExitCode(verboseErr, usage)
	}

	switch apr.Arg(0) {
	case dprocedures.DoltBackupParamAdd:
		if apr.NArg() != 3 {
			return HandleVErrAndExitCode(VerboseErrorUsage, usage)
		}
	case dprocedures.DoltBackupParamRemove,
		dprocedures.DoltBackupParamRm,
		dprocedures.DoltBackupParamSync,
		dprocedures.DoltBackupParamSyncUrl:
		if apr.NArg() != 2 {
			return HandleVErrAndExitCode(VerboseErrorUsage, usage)
		}
	case dprocedures.DoltBackupParamRestore:
		if apr.NArg() < 3 {
			return HandleVErrAndExitCode(VerboseErrorUsage, usage)
		}
	default:
		return HandleVErrAndExitCode(VerboseErrorUsage, usage)
	}

	verboseErr := callDoltBackupProc(&queryEngine, args)
	return HandleVErrAndExitCode(verboseErr, usage)
}

// callDoltBackupProc calls the dolt_backup stored procedure with the given parameters.
func callDoltBackupProc(queryEngine *cli.QueryEngineResult, params []string) errhand.VerboseError {
	query, err := interpolateStoredProcedureCall(dprocedures.DoltBackupProcedureName, params)
	if err != nil {
		return errhand.BuildDError("failed to interpolate stored procedure %s", dprocedures.DoltBackupProcedureName).AddCause(err).Build()
	}

	_, err = cli.GetRowsForSql(queryEngine.Queryist, queryEngine.Context, query)
	if err != nil {
		return errhand.BuildDError("failed to execute stored procedure %s", dprocedures.DoltBackupProcedureName).AddCause(err).Build()
	}

	return nil
}

// printDoltBackupsTable queries the dolt_backups table and prints the results. If the verbose flag is set, it prints
// name, url, and params columns. Otherwise, it prints only the name column.
func printDoltBackupsTable(queryEngine *cli.QueryEngineResult, apr *argparser.ArgParseResults) errhand.VerboseError {
	query := fmt.Sprintf("SELECT * FROM `%s`", doltdb.BackupsTableName)
	schema, rowItr, _, err := queryEngine.Queryist.Query(queryEngine.Context, query)
	if err != nil {
		return errhand.BuildDError("failed to execute query for %s", doltdb.BackupsTableName).AddCause(err).Build()
	}
	rows, err := sql.RowIterToRows(queryEngine.Context, rowItr)
	if err != nil {
		return errhand.BuildDError("failed to retrieve slice for %s", doltdb.BackupsTableName).AddCause(err).Build()
	}

	const colExpectedStrFmt = "column '%s': expected string, got %v"
	for _, row := range rows {
		name, ok := row[0].(string)
		if !ok {
			return errhand.BuildDError(colExpectedStrFmt, schema[0].Name, row[0]).Build()
		}

		if !apr.Contains(cli.VerboseFlag) {
			cli.Println(name)
			continue
		}

		url, ok := row[1].(string)
		if !ok {
			return errhand.BuildDError(colExpectedStrFmt, schema[1].Name, row[1]).Build()
		}

		jsonStr, err := getJsonAsString(queryEngine.Context, row[2])
		if err != nil {
			return errhand.BuildDError(colExpectedStrFmt, schema[2].Name, row[2]).AddCause(err).Build()
		}

		cli.Printf("%s %s %s\n", name, url, jsonStr)
	}

	return nil
}
