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
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var ErrInvalidPort = errors.New("invalid port")

var remoteDocs = cli.CommandDocumentationContent{
	ShortDesc: "Manage set of tracked repositories",
	LongDesc: `With no arguments, shows a list of existing remotes. Several subcommands are available to perform operations on the remotes.

{{.EmphasisLeft}}add{{.EmphasisRight}}
Adds a remote named {{.LessThan}}name{{.GreaterThan}} for the repository at {{.LessThan}}url{{.GreaterThan}}. The command dolt fetch {{.LessThan}}name{{.GreaterThan}} can then be used to create and update remote-tracking branches {{.EmphasisLeft}}<name>/<branch>{{.EmphasisRight}}.

The {{.LessThan}}url{{.GreaterThan}} parameter supports url schemes of http, https, aws, gs, and file. The url prefix defaults to https. If the {{.LessThan}}url{{.GreaterThan}} parameter is in the format {{.EmphasisLeft}}<organization>/<repository>{{.EmphasisRight}} then dolt will use the {{.EmphasisLeft}}remotes.default_host{{.EmphasisRight}} from your configuration file (Which will be dolthub.com unless changed).

AWS cloud remote urls should be of the form {{.EmphasisLeft}}aws://[dynamo-table:s3-bucket]/database{{.EmphasisRight}}.  You may configure your aws cloud remote using the optional parameters {{.EmphasisLeft}}aws-region{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-type{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-file{{.EmphasisRight}}.

aws-creds-type specifies the means by which credentials should be retrieved in order to access the specified cloud resources (specifically the dynamo table, and the s3 bucket). Valid values are 'role', 'env', or 'file'.

	role: Use the credentials installed for the current user
	env: Looks for environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	file: Uses the credentials file specified by the parameter aws-creds-file
	
GCP remote urls should be of the form gs://gcs-bucket/database and will use the credentials setup using the gcloud command line available from Google.

The local filesystem can be used as a remote by providing a repository url in the format file://absolute path. See https://en.wikipedia.org/wiki/File_URI_scheme

{{.EmphasisLeft}}remove{{.EmphasisRight}}, {{.EmphasisLeft}}rm{{.EmphasisRight}}
Remove the remote named {{.LessThan}}name{{.GreaterThan}}. All remote-tracking branches and configuration settings for the remote are removed.`,

	Synopsis: []string{
		"[-v | --verbose]",
		"add [--aws-region {{.LessThan}}region{{.GreaterThan}}] [--aws-creds-type {{.LessThan}}creds-type{{.GreaterThan}}] [--aws-creds-file {{.LessThan}}file{{.GreaterThan}}] [--aws-creds-profile {{.LessThan}}profile{{.GreaterThan}}] {{.LessThan}}name{{.GreaterThan}} {{.LessThan}}url{{.GreaterThan}}",
		"remove {{.LessThan}}name{{.GreaterThan}}",
	},
}

const (
	addRemoteId         = "add"
	removeRemoteId      = "remove"
	removeRemoteShortId = "rm"
)

var awsParams = []string{dbfactory.AWSRegionParam, dbfactory.AWSCredsTypeParam, dbfactory.AWSCredsFileParam, dbfactory.AWSCredsProfile}
var credTypes = dbfactory.AWSCredTypes

type RemoteCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RemoteCmd) Name() string {
	return "remote"
}

// Description returns a description of the command
func (cmd RemoteCmd) Description() string {
	return "Manage set of tracked repositories."
}

func (cmd RemoteCmd) GatedForNBF(nbf *types.NomsBinFormat) bool {
	return types.IsFormat_DOLT_1(nbf)
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd RemoteCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, remoteDocs, ap))
}

func (cmd RemoteCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"region", "cloud provider region associated with this remote."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"creds-type", "credential type.  Valid options are role, env, and file.  See the help section for additional details."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"profile", "AWS profile to use."})
	ap.SupportsFlag(verboseFlag, "v", "When printing the list of remotes adds additional details.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, credTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use")
	return ap
}

// EventType returns the type of the event to log
func (cmd RemoteCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_REMOTE
}

// Exec executes the command
func (cmd RemoteCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, remoteDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	var verr errhand.VerboseError

	switch {
	case apr.NArg() == 0:
		verr = printRemotes(dEnv, apr)
	case apr.Arg(0) == addRemoteId:
		verr = addRemote(dEnv, apr)
	case apr.Arg(0) == removeRemoteId:
		verr = removeRemote(ctx, dEnv, apr)
	case apr.Arg(0) == removeRemoteShortId:
		verr = removeRemote(ctx, dEnv, apr)
	default:
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func removeRemote(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	old := strings.TrimSpace(apr.Arg(1))
	err := dEnv.RemoveRemote(ctx, old)

	switch err {
	case nil:
		return nil
	case env.ErrFailedToWriteRepoState:
		return errhand.BuildDError("error: failed to save change to repo state").AddCause(err).Build()
	case env.ErrFailedToDeleteRemote:
		return errhand.BuildDError("error: failed to delete remote tracking ref").AddCause(err).Build()
	case env.ErrFailedToReadFromDb:
		return errhand.BuildDError("error: failed to read from db").AddCause(err).Build()
	case env.ErrRemoteNotFound:
		return errhand.BuildDError("error: unknown remote: '%s' ", old).Build()
	default:
		return errhand.BuildDError("error: unknown error").AddCause(err).Build()
	}
}

func addRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
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

	r := env.NewRemote(remoteName, remoteUrl, params, dEnv)
	err = dEnv.AddRemote(r.Name, r.Url, r.FetchSpecs, r.Params)

	switch err {
	case nil:
		return nil
	case env.ErrRemoteAlreadyExists:
		return errhand.BuildDError("error: a remote named '%s' already exists.", r.Name).AddDetails("remove it before running this command again").Build()
	case env.ErrRemoteNotFound:
		return errhand.BuildDError("error: unknown remote: '%s' ", r.Name).Build()
	case env.ErrInvalidRemoteURL:
		return errhand.BuildDError("error: '%s' is not valid.", r.Url).AddCause(err).Build()
	case env.ErrInvalidRemoteName:
		return errhand.BuildDError("error: invalid remote name: " + r.Name).Build()
	default:
		return errhand.BuildDError("error: Unable to save changes.").AddCause(err).Build()
	}
}

func parseRemoteArgs(apr *argparser.ArgParseResults, scheme, remoteUrl string) (map[string]string, errhand.VerboseError) {
	params := map[string]string{}

	var verr errhand.VerboseError
	if scheme == dbfactory.AWSScheme {
		verr = addAWSParams(remoteUrl, apr, params)
	} else {
		verr = verifyNoAwsParams(apr)
	}

	return params, verr
}

func addAWSParams(remoteUrl string, apr *argparser.ArgParseResults, params map[string]string) errhand.VerboseError {
	isAWS := strings.HasPrefix(remoteUrl, "aws")

	if !isAWS {
		for _, p := range awsParams {
			if _, ok := apr.GetValue(p); ok {
				return errhand.BuildDError(p + " param is only valid for aws cloud remotes in the format aws://dynamo-table:s3-bucket/database").Build()
			}
		}
	}

	for _, p := range awsParams {
		if val, ok := apr.GetValue(p); ok {
			params[p] = val
		}
	}

	return nil
}

func verifyNoAwsParams(apr *argparser.ArgParseResults) errhand.VerboseError {
	if awsParams := apr.GetValues(awsParams...); len(awsParams) > 0 {
		awsParamKeys := make([]string, 0, len(awsParams))
		for k := range awsParams {
			awsParamKeys = append(awsParamKeys, k)
		}

		keysStr := strings.Join(awsParamKeys, ",")
		return errhand.BuildDError("The parameters %s, are only valid for aws remotes", keysStr).SetPrintUsage().Build()
	}

	return nil
}

func printRemotes(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	remotes, err := dEnv.GetRemotes()

	if err != nil {
		return errhand.BuildDError("Unable to get remotes from the local directory").AddCause(err).Build()
	}

	for _, r := range remotes {
		if apr.Contains(verboseFlag) {
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
