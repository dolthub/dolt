// Copyright 2019 Liquidata, Inc.
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
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/dolt/go/libraries/utils/earl"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
)

var ErrInvalidPort = errors.New("invalid port")

var remoteShortDesc = "Manage set of tracked repositories"
var remoteLongDesc = `With no arguments, shows a list of existing remotes. Several subcommands are available to perform operations on the remotes.

{{.EmphasisLeft}}add{{.EmphasisRight}}
Adds a remote named {{.LessThan}}name{{.GreaterThan}} for the repository at {{.LessThan}}url{{.GreaterThan}}. The command dolt fetch {{.LessThan}}name{{.GreaterThan}} can then be used to create and update remote-tracking branches {{.EmphasisLeft}}<name>/<branch>{{.EmphasisRight}}.

The {{.LessThan}}url{{.GreaterThan}} parameter supports url schemes of http, https, aws, gs, and file.  If a url scheme does not prefix the url then https is assumed.  If the {{.LessThan}}url{{.GreaterThan}} paramenter is in the format {{.EmphasisLeft}}<organization>/<repository>{{.EmphasisRight}} then dolt will use the {{.EmphasisLeft}}remotes.default_host{{.EmphasisRight}} from your configuration file (Which will be dolthub.com unless changed).

AWS cloud remote urls should be of the form {{.EmphasisLeft}}aws://[dynamo-table:s3-bucket]/database{{.EmphasisRight}}.  You may configure your aws cloud remote using the optional parameters {{.EmphasisLeft}}aws-region{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-type{{.EmphasisRight}}, {{.EmphasisLeft}}aws-creds-file{{.EmphasisRight}}.

aws-creds-type specifies the means by which credentials should be retrieved in order to access the specified cloud resources (specifically the dynamo table, and the s3 bucket). Valid values are 'role', 'env', or 'file'.

	\trole: Use the credentials installed for the current user
	\tenv: Looks for environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	\tfile: Uses the credentials file specified by the parameter aws-creds-file
	
GCP remote urls should be of the form gs://gcs-bucket/database and will use the credentials setup using the gcloud command line available from Google +

The local filesystem can be used as a remote by providing a repository url in the format file://absolute path. See https://en.wikipedia.org/wiki/File_URI_schemethi
{{.EmphasisLeft}}remove{{.EmphasisRight}}, {{.EmphasisLeft}}rm{{.EmphasisRight}}, 
Remove the remote named {{.LessThan}}name{{.GreaterThan}}. All remote-tracking branches and configuration settings for the remote are removed.`

var remoteSynopsis = []string{
	"[-v | --verbose]",
	"add [--aws-region {{.LessThan}}region{{.GreaterThan}}] [--aws-creds-type {{.LessThan}}creds-type{{.GreaterThan}}] [--aws-creds-file {{.LessThan}}file{{.GreaterThan}}] [--aws-creds-profile {{.LessThan}}profile{{.GreaterThan}}] {{.LessThan}}name{{.GreaterThan}} {{.LessThan}}url{{.GreaterThan}}",
	"remove {{.LessThan}}name{{.GreaterThan}}",
}

var remoteDocumentation = cli.CommandDocumentation{
	ShortDesc: pushShortDesc,
	LongDesc: pushLongDesc,
	Synopsis: pushSynopsis,
}

const (
	addRemoteId    = "add"
	removeRemoteId = "remove"
)

var awsParams = []string{dbfactory.AWSRegionParam, dbfactory.AWSCredsTypeParam, dbfactory.AWSCredsFileParam, dbfactory.AWSCredsProfile}
var credTypes = []string{dbfactory.RoleCS.String(), dbfactory.EnvCS.String(), dbfactory.FileCS.String()}

type RemoteCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RemoteCmd) Name() string {
	return "remote"
}

// Description returns a description of the command
func (cmd RemoteCmd) Description() string {
	return "Manage set of tracked repositories."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd RemoteCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, commandStr, remoteDocumentation, ap)
}

func (cmd RemoteCmd) createArgParser() *argparser.ArgParser {
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
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, remoteDocumentation, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError

	switch {
	case apr.NArg() == 0:
		verr = printRemotes(dEnv, apr)
	case apr.Arg(0) == addRemoteId:
		verr = addRemote(dEnv, apr)
	case apr.Arg(0) == removeRemoteId:
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

	remotes, err := dEnv.GetRemotes()

	if err != nil {
		return errhand.BuildDError("error: unable to read remotes").Build()
	}

	if _, ok := remotes[old]; !ok {
		return errhand.BuildDError("error: unknown remote " + old).Build()
	}

	refs, err := dEnv.DoltDB.GetRefsOfType(ctx, map[ref.RefType]struct{}{ref.RemoteRefType: {}})

	if err != nil {
		return errhand.BuildDError("error: failed to read from db").AddCause(err).Build()
	}

	for _, r := range refs {
		rr := r.(ref.RemoteRef)

		if rr.GetRemote() == old {
			err = dEnv.DoltDB.DeleteBranch(ctx, rr)

			if err != nil {
				return errhand.BuildDError("error: failed to delete remote tracking ref '%s'", rr.String()).Build()
			}
		}
	}

	delete(dEnv.RepoState.Remotes, old)
	err = dEnv.RepoState.Save(dEnv.FS)

	if err != nil {
		return errhand.BuildDError("error: unable to save changes.").AddCause(err).Build()
	}

	return nil
}

func getAbsRemoteUrl(fs filesys.Filesys, cfg config.ReadableConfig, urlArg string) (string, string, error) {
	u, err := earl.Parse(urlArg)

	if err != nil {
		return "", "", err
	}

	if u.Scheme != "" {
		if u.Scheme == dbfactory.FileScheme {
			absUrl, err := getAbsFileRemoteUrl(u.Host+u.Path, fs)

			if err != nil {
				return "", "", err
			}

			return dbfactory.FileScheme, absUrl, err
		}

		return u.Scheme, urlArg, nil
	} else if u.Host != "" {
		return dbfactory.HTTPSScheme, "https://" + urlArg, nil
	}

	hostName, err := cfg.GetString(env.RemotesApiHostKey)

	if err != nil {
		if err != config.ErrConfigParamNotFound {
			return "", "", err
		}

		hostName = env.DefaultRemotesApiHost
	}

	hostName = strings.TrimSpace(hostName)

	return dbfactory.HTTPSScheme, "https://" + path.Join(hostName, u.Path), nil
}

func getAbsFileRemoteUrl(urlStr string, fs filesys.Filesys) (string, error) {
	var err error
	urlStr = filepath.Clean(urlStr)
	urlStr, err = fs.Abs(urlStr)

	if err != nil {
		return "", err
	}

	exists, isDir := fs.Exists(urlStr)

	if !exists {
		return "", os.ErrNotExist
	} else if !isDir {
		return "", filesys.ErrIsFile
	}

	urlStr = strings.ReplaceAll(urlStr, `\`, "/")
	if !strings.HasPrefix(urlStr, "/") {
		urlStr = "/" + urlStr
	}
	return dbfactory.FileScheme + "://" + urlStr, nil
}

func addRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	remoteName := strings.TrimSpace(apr.Arg(1))

	if strings.IndexAny(remoteName, " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|") != -1 {
		return errhand.BuildDError("invalid remote name: " + remoteName).Build()
	}

	if _, ok := dEnv.RepoState.Remotes[remoteName]; ok {
		return errhand.BuildDError("error: A remote named '%s' already exists.", remoteName).AddDetails("remove it before running this command again").Build()
	}

	remoteUrl := apr.Arg(2)
	scheme, remoteUrl, err := getAbsRemoteUrl(dEnv.FS, dEnv.Config, remoteUrl)

	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", remoteUrl).Build()
	}

	params, verr := parseRemoteArgs(apr, scheme, remoteUrl)

	if verr != nil {
		return verr
	}

	r := env.NewRemote(remoteName, remoteUrl, params)
	dEnv.RepoState.AddRemote(r)
	err = dEnv.RepoState.Save(dEnv.FS)

	if err != nil {
		return errhand.BuildDError("error: Unable to save changes.").AddCause(err).Build()
	}

	return nil
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
