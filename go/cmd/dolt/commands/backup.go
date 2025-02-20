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
	"encoding/json"
	"strings"

	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas/pull"
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

	var verr errhand.VerboseError

	// All the sub commands except `restore` require a valid environment
	if apr.NArg() == 0 || apr.Arg(0) != cli.RestoreBackupId {
		if !cli.CheckEnvIsValid(dEnv) {
			return 2
		}
	}

	switch {
	case apr.NArg() == 0:
		verr = printBackups(dEnv, apr)
	case apr.Arg(0) == cli.AddBackupId:
		verr = addBackup(dEnv, apr)
	case apr.Arg(0) == cli.RemoveBackupId:
		verr = removeBackup(ctx, dEnv, apr)
	case apr.Arg(0) == cli.RemoveBackupShortId:
		verr = removeBackup(ctx, dEnv, apr)
	case apr.Arg(0) == cli.SyncBackupId:
		verr = syncBackup(ctx, dEnv, apr)
	case apr.Arg(0) == cli.SyncBackupUrlId:
		verr = syncBackupUrl(ctx, dEnv, apr)
	case apr.Arg(0) == cli.RestoreBackupId:
		verr = restoreBackup(ctx, dEnv, apr)
	default:
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func removeBackup(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	old := strings.TrimSpace(apr.Arg(1))
	err := dEnv.RemoveBackup(ctx, old)

	switch err {
	case nil:
		return nil
	case env.ErrFailedToWriteRepoState:
		return errhand.BuildDError("error: failed to save change to repo state").AddCause(err).Build()
	case env.ErrFailedToDeleteBackup:
		return errhand.BuildDError("error: failed to delete backup tracking ref").AddCause(err).Build()
	case env.ErrFailedToReadFromDb:
		return errhand.BuildDError("error: failed to read from db").AddCause(err).Build()
	case env.ErrBackupNotFound:
		return errhand.BuildDError("error: unknown backup: '%s' ", old).Build()
	default:
		return errhand.BuildDError("error: unknown error").AddCause(err).Build()
	}
}

func addBackup(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	backupName := strings.TrimSpace(apr.Arg(1))

	backupUrl := apr.Arg(2)
	scheme, absBackupUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, backupUrl)
	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", backupUrl).AddCause(err).Build()
	}

	params, err := cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	r := env.NewRemote(backupName, backupUrl, params)
	err = dEnv.AddBackup(r)

	switch err {
	case nil:
		return nil
	case env.ErrBackupAlreadyExists:
		return errhand.BuildDError("error: a backup named '%s' already exists.", r.Name).AddDetails("remove it before running this command again").Build()
	case env.ErrBackupNotFound:
		return errhand.BuildDError("error: unknown backup: '%s' ", r.Name).Build()
	case env.ErrInvalidBackupURL:
		return errhand.BuildDError("error: '%s' is not valid.", r.Url).AddCause(err).Build()
	case env.ErrInvalidBackupName:
		return errhand.BuildDError("error: invalid backup name: " + r.Name).Build()
	default:
		return errhand.BuildDError("error: Unable to save changes.").AddCause(err).Build()
	}
}

func printBackups(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	backups, err := dEnv.GetBackups()
	if err != nil {
		return errhand.BuildDError("Unable to get backups from the local directory").AddCause(err).Build()
	}

	for _, r := range backups.Snapshot() {
		if apr.Contains(cli.VerboseFlag) {
			paramStr := make([]byte, 0)
			if len(r.Params) > 0 {
				paramStr, _ = json.Marshal(r.Params)
			}

			cli.Printf("%s %s %s\n", r.Name, r.Url, paramStr)
		} else {
			cli.Println(r.Name)
		}
	}

	return nil
}

func syncBackupUrl(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	backupUrl := apr.Arg(1)
	scheme, absBackupUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, backupUrl)
	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", backupUrl).AddCause(err).Build()
	}

	params, err := cli.ProcessBackupArgs(apr, scheme, absBackupUrl)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	b := env.NewRemote("__temp__", backupUrl, params)
	return backup(ctx, dEnv, b)
}

func syncBackup(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	backupName := strings.TrimSpace(apr.Arg(1))

	backups, err := dEnv.GetBackups()
	if err != nil {
		return errhand.BuildDError("Unable to get backups from the local directory").AddCause(err).Build()
	}

	b, ok := backups.Get(backupName)
	if !ok {
		return errhand.BuildDError("error: unknown backup: '%s' ", backupName).Build()
	}

	return backup(ctx, dEnv, b)
}

func backup(ctx context.Context, dEnv *env.DoltEnv, b env.Remote) errhand.VerboseError {
	destDb, err := b.GetRemoteDB(ctx, dEnv.DoltDB(ctx).ValueReadWriter().Format(), dEnv)
	if err != nil {
		return errhand.BuildDError("error: unable to open destination.").AddCause(err).Build()
	}
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return errhand.BuildDError("error: ").AddCause(err).Build()
	}
	err = actions.SyncRoots(ctx, dEnv.DoltDB(ctx), destDb, tmpDir, buildProgStarter(defaultLanguage), stopProgFuncs)

	switch err {
	case nil:
		return nil
	case pull.ErrDBUpToDate:
		return nil
	case env.ErrBackupAlreadyExists:
		return errhand.BuildDError("error: a backup named '%s' already exists.", b.Name).AddDetails("remove it before running this command again").Build()
	case env.ErrBackupNotFound:
		return errhand.BuildDError("error: unknown backup: '%s' ", b.Name).Build()
	case env.ErrInvalidBackupURL:
		return errhand.BuildDError("error: '%s' is not valid.", b.Url).AddCause(err).Build()
	case env.ErrInvalidBackupName:
		return errhand.BuildDError("error: invalid backup name: " + b.Name).Build()
	default:
		return errhand.BuildDError("error: Unable to save changes.").AddCause(err).Build()
	}
}

func restoreBackup(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() < 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}
	apr.Args = apr.Args[1:]
	restoredDB, urlStr, verr := parseArgs(apr)
	if verr != nil {
		return verr
	}

	// For error recovery, record whether EnvForClone created the directory, or just `.dolt/noms` within the directory.
	userDirExisted, _ := dEnv.FS.Exists(restoredDB)

	force := apr.Contains(cli.ForceFlag)

	scheme, remoteUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, urlStr)
	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", urlStr).Build()
	}

	var params map[string]string
	params, verr = parseRemoteArgs(apr, scheme, remoteUrl)
	if verr != nil {
		return verr
	}

	r := env.NewRemote("", remoteUrl, params)
	srcDb, err := r.GetRemoteDB(ctx, types.Format_Default, dEnv)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		return errhand.BuildDError("error: Unable to list databases").AddCause(err).Build()
	}
	var existingDEnv *env.DoltEnv
	err = mrEnv.Iter(func(dbName string, dEnv *env.DoltEnv) (stop bool, err error) {
		if dbName == restoredDB {
			existingDEnv = dEnv
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return errhand.BuildDError("error: Unable to list databases").AddCause(err).Build()
	}

	if existingDEnv != nil {
		if !force {
			return errhand.BuildDError("error: cannot restore backup into " + restoredDB + ". A database with that name already exists. Did you mean to supply --force?").Build()
		}

		tmpDir, err := existingDEnv.TempTableFilesDir()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		err = actions.SyncRoots(ctx, srcDb, existingDEnv.DoltDB(ctx), tmpDir, buildProgStarter(downloadLanguage), stopProgFuncs)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
	} else {
		// Create a new Dolt env for the clone; use env.NoRemote to avoid origin upstream
		clonedEnv, err := actions.EnvForClone(ctx, srcDb.ValueReadWriter().Format(), env.NoRemote, restoredDB, dEnv.FS, dEnv.Version, env.GetCurrentUserHomeDir)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}

		// Nil out the old Dolt env so we don't accidentally use the wrong database
		dEnv = nil

		// still make empty repo state
		_, err = env.CreateRepoState(clonedEnv.FS, env.DefaultInitBranch)
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		tmpDir, err := clonedEnv.TempTableFilesDir()
		if err != nil {
			return errhand.VerboseErrorFromError(err)
		}
		err = actions.SyncRoots(ctx, srcDb, clonedEnv.DoltDB(ctx), tmpDir, buildProgStarter(downloadLanguage), stopProgFuncs)
		if err != nil {
			// If we're cloning into a directory that already exists do not erase it. Otherwise
			// make best effort to delete the directory we created.
			if userDirExisted {
				_ = clonedEnv.FS.Delete(dbfactory.DoltDir, true)
			} else {
				_ = clonedEnv.FS.Delete(".", true)
			}
			return errhand.VerboseErrorFromError(err)
		}
	}

	return nil
}
