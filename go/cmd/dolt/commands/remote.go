// Copyright 2019 Dolthub, Inc.
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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/gitremote"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var ErrInvalidPort = errors.New("invalid port")

var remoteDocs = cli.CommandDocumentationContent{
	ShortDesc: "Manage set of tracked repositories",
	LongDesc: `With no arguments, shows a list of existing remotes. Several subcommands are available to perform operations on the remotes.

{{.EmphasisLeft}}add{{.EmphasisRight}}
Adds a remote named {{.LessThan}}name{{.GreaterThan}} for the repository at {{.LessThan}}url{{.GreaterThan}}. The command dolt fetch {{.LessThan}}name{{.GreaterThan}} can then be used to create and update remote-tracking branches {{.EmphasisLeft}}<name>/<branch>{{.EmphasisRight}}.

The {{.LessThan}}url{{.GreaterThan}} parameter supports url schemes of http, https, aws, gs, git, and file. The url prefix defaults to https. If the {{.LessThan}}url{{.GreaterThan}} parameter is in the format {{.EmphasisLeft}}<organization>/<repository>{{.EmphasisRight}} then dolt will use the {{.EmphasisLeft}}remotes.default_host{{.EmphasisRight}} from your configuration file (Which will be dolthub.com unless changed).

{{.EmphasisLeft}}Git Remotes{{.EmphasisRight}}
You can use any git repository as a dolt remote by using the git:// scheme or an HTTP(S) URL ending in .git:

    dolt remote add origin git://github.com/user/repo.git
    dolt remote add origin https://github.com/user/repo.git

Git credentials are automatically detected from:
  - ssh-agent and ~/.ssh configuration
  - Git credential helper / OS keychain
  - ~/.netrc file

Before using a git remote, initialize it with {{.EmphasisLeft}}dolt remote init{{.EmphasisRight}}.

AWS cloud remote urls should be of the form {{.EmphasisLeft}}aws://[dynamo-table:s3-bucket]/database{{.EmphasisRight}}.  You may configure your aws cloud remote using the optional parameters {{.EmphasisLeft}}aws-region{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-type{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-file{{.EmphasisRight}}.

aws-creds-type specifies the means by which credentials should be retrieved in order to access the specified cloud resources (specifically the dynamo table, and the s3 bucket). Valid values are 'role', 'env', or 'file'.

	role: Use the credentials installed for the current user
	env: Looks for environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	file: Uses the credentials file specified by the parameter aws-creds-file
	
GCP remote urls should be of the form gs://gcs-bucket/database and will use the credentials setup using the gcloud command line available from Google.

The local filesystem can be used as a remote by providing a repository url in the format file://absolute path. See https://en.wikipedia.org/wiki/File_URI_scheme

{{.EmphasisLeft}}remove{{.EmphasisRight}}, {{.EmphasisLeft}}rm{{.EmphasisRight}}
Remove the remote named {{.LessThan}}name{{.GreaterThan}}. All remote-tracking branches and configuration settings for the remote are removed.

{{.EmphasisLeft}}init{{.EmphasisRight}}
Initialize a git repository as a dolt remote. This creates the necessary directory structure on a custom git ref (default: refs/dolt/data) to store dolt data. The git repository URL must end with .git or use the git:// scheme.

This command is idempotent - it's safe to run multiple times on the same repository. Dolt data is stored on a custom ref that doesn't interfere with normal git branches.

Example:
    dolt remote init https://github.com/user/repo.git
    dolt remote init --ref refs/dolt/custom https://github.com/user/repo.git`,

	Synopsis: []string{
		"[-v | --verbose]",
		"add [--aws-region {{.LessThan}}region{{.GreaterThan}}] [--aws-creds-type {{.LessThan}}creds-type{{.GreaterThan}}] [--aws-creds-file {{.LessThan}}file{{.GreaterThan}}] [--aws-creds-profile {{.LessThan}}profile{{.GreaterThan}}] {{.LessThan}}name{{.GreaterThan}} {{.LessThan}}url{{.GreaterThan}}",
		"remove {{.LessThan}}name{{.GreaterThan}}",
		"init [--ref {{.LessThan}}ref-name{{.GreaterThan}}] {{.LessThan}}git-url{{.GreaterThan}}",
	},
}

const (
	addRemoteId         = "add"
	removeRemoteId      = "remove"
	removeRemoteShortId = "rm"
	initRemoteId        = "init"

	// gitRefFlag is the flag for specifying a custom git ref
	gitRefFlag = "ref"
)

type RemoteCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RemoteCmd) Name() string {
	return "remote"
}

// Description returns a description of the command
func (cmd RemoteCmd) Description() string {
	return "Manage set of tracked repositories."
}

func (cmd RemoteCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(remoteDocs, ap)
}

func (cmd RemoteCmd) ArgParser() *argparser.ArgParser {
	ap := cli.CreateRemoteArgParser()
	ap.SupportsFlag(cli.VerboseFlag, "v", "When printing the list of remotes adds additional details.")

	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "Cloud provider region associated with this remote.")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "Credential type. Valid options are role, env, and file. See the help section for additional details.", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, dbfactory.AWSCredTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use")

	ap.SupportsString(dbfactory.OSSCredsFileParam, "", "file", "OSS credentials file")
	ap.SupportsString(dbfactory.OSSCredsProfile, "", "profile", "OSS profile to use")

	// Git remote init flags
	ap.SupportsString(gitRefFlag, "", "ref-name", "Custom git ref for dolt data (default: refs/dolt/data)")
	return ap
}

// EventType returns the type of the event to log
func (cmd RemoteCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_REMOTE
}

// RequiresRepo returns false because `dolt remote init` can be run without a dolt repository.
// Other subcommands still require a repository context.
func (cmd RemoteCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd RemoteCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, remoteDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	// Handle `dolt remote init` separately since it doesn't require a dolt repository
	if apr.NArg() > 0 && apr.Arg(0) == initRemoteId {
		verr := initGitRemote(ctx, apr)
		return HandleVErrAndExitCode(verr, usage)
	}

	// All other subcommands require a dolt repository
	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	var verr errhand.VerboseError
	switch {
	case apr.NArg() == 0:
		verr = printRemotes(queryist.Context, queryist.Queryist, apr)
	case apr.Arg(0) == addRemoteId:
		verr = addRemote(queryist.Context, queryist.Queryist, dEnv, apr)
	case apr.Arg(0) == removeRemoteId, apr.Arg(0) == removeRemoteShortId:
		verr = removeRemote(queryist.Context, queryist.Queryist, apr)
	default:
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func removeRemote(sqlCtx *sql.Context, qureyist cli.Queryist, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}
	toRemove := strings.TrimSpace(apr.Arg(1))

	err := callSQLRemoteRemove(sqlCtx, qureyist, toRemove)
	if err != nil {
		return errhand.BuildDError("error: Unable to remove remote.").AddCause(err).Build()
	}
	return nil
}

func addRemote(sqlCtx *sql.Context, queryist cli.Queryist, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	remoteName := strings.TrimSpace(apr.Arg(1))

	remoteUrl := apr.Arg(2)

	scheme, absRemoteUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, remoteUrl)
	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", remoteUrl).AddCause(err).Build()
	}
	params, verr := parseRemoteArgs(apr, scheme, absRemoteUrl)
	if verr != nil {
		return verr
	}

	if len(params) == 0 {
		err := callSQLRemoteAdd(sqlCtx, queryist, remoteName, absRemoteUrl)
		if err != nil {
			return errhand.BuildDError("error: Unable to add remote.").AddCause(err).Build()
		}
	} else {
		// We only support adding remotes with parameters in the local configuration
		if _, ok := queryist.(*engine.SqlEngine); !ok {
			return errhand.BuildDError("error: remote add failed. sql-server running while attempting to use advanced remote parameters. Stop server and re-run").Build()
		}
		return addRemoteLocaly(remoteName, absRemoteUrl, params, dEnv)
	}
	return nil
}

// addRemoteLocal adds a remote to the local configuration, which should only be used in the event that there
// are AWS/GCP/OSS parameters. These are not supported in the SQL interface
func addRemoteLocaly(remoteName, remoteUrl string, params map[string]string, dEnv *env.DoltEnv) errhand.VerboseError {
	rmot := env.NewRemote(remoteName, remoteUrl, params)
	err := dEnv.AddRemote(rmot)

	switch err {
	case nil:
		return nil
	case env.ErrRemoteAlreadyExists:
		return errhand.BuildDError("error: a remote named '%s' already exists.", rmot.Name).AddDetails("remove it before running this command again").Build()
	case env.ErrRemoteNotFound:
		return errhand.BuildDError("error: unknown remote: '%s' ", rmot.Name).Build()
	case env.ErrInvalidRemoteURL:
		return errhand.BuildDError("error: '%s' is not valid.", rmot.Url).AddCause(err).Build()
	case env.ErrInvalidRemoteName:
		return errhand.BuildDError("error: invalid remote name: %s", rmot.Name).Build()
	default:
		return errhand.BuildDError("error: Unable to save changes.").AddCause(err).Build()
	}
}

func parseRemoteArgs(apr *argparser.ArgParseResults, scheme, remoteUrl string) (map[string]string, errhand.VerboseError) {
	params := map[string]string{}

	var err error
	switch scheme {
	case dbfactory.AWSScheme:
		err = cli.AddAWSParams(remoteUrl, apr, params)
	case dbfactory.OSSScheme:
		err = cli.AddOSSParams(remoteUrl, apr, params)
	default:
		err = cli.VerifyNoAwsParams(apr)
	}
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	return params, nil
}

// callSQLRemoteAdd calls the SQL function `call `dolt_remote('add', remoteName, remoteUrl)`
func callSQLRemoteAdd(sqlCtx *sql.Context, queryist cli.Queryist, remoteName, remoteUrl string) error {
	qry, err := dbr.InterpolateForDialect("call dolt_remote('add', ?, ?)", []interface{}{remoteName, remoteUrl}, dialect.MySQL)
	if err != nil {
		return err
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtx, qry)
	return err
}

// callSQLRemoteRemove calls the SQL function `call `dolt_remote('remove', remoteName)`
func callSQLRemoteRemove(sqlCtxe *sql.Context, queryist cli.Queryist, remoteName string) error {
	qry, err := dbr.InterpolateForDialect("call dolt_remote('remove', ?)", []interface{}{remoteName}, dialect.MySQL)
	if err != nil {
		return err
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtxe, qry)
	return err
}

type remote struct {
	Name   string
	Url    string
	Params string
}

func getRemotesSQL(sqlCtx *sql.Context, queryist cli.Queryist) ([]remote, error) {
	qry := "select name,url,params from dolt_remotes"
	rows, err := cli.GetRowsForSql(queryist, sqlCtx, qry)
	if err != nil {
		return nil, err
	}

	remotes := make([]remote, 0, len(rows))
	for _, r := range rows {
		name, ok := r[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid remote name")
		}

		url, ok := r[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid remote url")
		}

		params, err := getJsonAsString(sqlCtx, r[2])
		if err != nil {
			return nil, fmt.Errorf("invalid params")
		}

		remotes = append(remotes, remote{
			Name:   name,
			Url:    url,
			Params: params,
		})
	}

	return remotes, nil
}

// getJsonAsString returns a string representation of the given interface{}, which can either be a string or a JSONDocument.
// If it is a string, it gets returned as is. If it is a JSONDocument, it gets converted to a string.
// SQLEngine and the remote connection behave a little differently here, which is the reason for needing this.
func getJsonAsString(sqlCtx *sql.Context, params interface{}) (string, error) {
	switch p := params.(type) {
	case string:
		return p, nil
	case sql.JSONWrapper:
		json, err := types.JsonToMySqlString(sqlCtx, p)
		if err != nil {
			return "", err
		}
		if json == "{}" {
			return "", nil
		}
		return json, nil
	default:
		return "", fmt.Errorf("unexpected interface{} type: %T", p)
	}
}

func printRemotes(sqlCtx *sql.Context, queryist cli.Queryist, apr *argparser.ArgParseResults) errhand.VerboseError {
	remotes, err := getRemotesSQL(sqlCtx, queryist)
	if err != nil {
		return errhand.BuildDError("Unable to get remotes from the local directory").AddCause(err).Build()
	}

	for _, r := range remotes {
		if apr.Contains(cli.VerboseFlag) {
			cli.Printf("%s %s %s\n", r.Name, r.Url, r.Params)
		} else {
			cli.Println(r.Name)
		}
	}

	return nil
}

// initGitRemote initializes a git repository as a dolt remote by creating the
// .dolt_remote/ directory structure on a custom git ref.
func initGitRemote(ctx context.Context, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("usage: dolt remote init <git-url>").SetPrintUsage().Build()
	}

	gitURL := strings.TrimSpace(apr.Arg(1))

	// Validate that this is a git URL
	if !dbfactory.IsGitURL(gitURL) {
		return errhand.BuildDError("error: '%s' is not a valid git remote URL", gitURL).
			AddDetails("Git remote URLs must use the git:// scheme or end with .git").Build()
	}

	// Get custom ref if specified
	ref := gitremote.DefaultRef
	if refVal, ok := apr.GetValue(gitRefFlag); ok {
		ref = refVal
	}

	cli.Printf("Initializing git remote at %s on ref %s...\n", gitURL, ref)

	// Create a temporary directory for git operations
	localPath, err := os.MkdirTemp("", "dolt-remote-init-*")
	if err != nil {
		return errhand.BuildDError("error: failed to create temp directory").AddCause(err).Build()
	}
	defer os.RemoveAll(localPath)

	// Open the repository
	repo, err := gitremote.Open(ctx, gitremote.OpenOptions{
		URL:       gitURL,
		Ref:       ref,
		LocalPath: localPath,
	})
	if err != nil {
		return errhand.BuildDError("error: failed to open git repository").AddCause(err).Build()
	}
	defer repo.Close()

	// Checkout the ref to populate the worktree (if it exists)
	if err := repo.CheckoutRef(ctx); err != nil {
		return errhand.BuildDError("error: failed to checkout ref").AddCause(err).Build()
	}

	// Check if already initialized
	initialized, err := repo.IsInitialized()
	if err != nil {
		return errhand.BuildDError("error: failed to check if remote is initialized").AddCause(err).Build()
	}

	if initialized {
		cli.Println("Remote is already initialized for dolt.")
		return nil
	}

	// Initialize the remote structure
	if err := repo.InitRemote(ctx); err != nil {
		return errhand.BuildDError("error: failed to initialize git remote").AddCause(err).Build()
	}

	cli.Println("Successfully initialized git repository as dolt remote.")
	cli.Printf("You can now add this remote with: dolt remote add <name> %s\n", gitURL)

	return nil
}
