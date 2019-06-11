package commands

import (
	"encoding/json"
	"errors"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dbfactory"
	"path"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/config"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/earl"
)

var ErrInvalidPort = errors.New("invalid port")

var remoteShortDesc = "Manage set of tracked repositories"
var remoteLongDesc = "With no arguments, shows a list of existing remotes. Several subcommands are available to perform " +
	"operations on the remotes." +
	"\n" +
	"\n<b>add</b>\n" +
	"Adds a remote named <name> for the repository at <url>. The command dolt fetch <name> can " +
	"then be used to create and update remote-tracking branches <name>/<branch>." +
	"\n" +
	"\nThe <url> parameter supports url schemes of http, https, aws, gs, and file.  If a url scheme does not prefix the " +
	"url then https is assumed.  If the <url> paramenter is in the format <organization>/<repository> then dolt will use " +
	"the remotes.default_host from your configuration file (Which will be dolthub.com unless changed).\n" +
	"\n" +
	"AWS cloud remote urls should be of the form aws://dynamo-table:s3-bucket/database.  You may configure your aws " +
	"cloud remote using the optional parameters aws-region, aws-creds-type, aws-creds-file.\n" +
	"\n" +
	"aws-creds-type specifies the means by which credentials should be retrieved in order to access the specified " +
	"cloud resources (specifically the dynamo table, and the s3 bucket). Valid values are 'role', 'env', or 'file'.\n" +
	"\n" +
	"\trole: Use the credentials installed for the current user\n" +
	"\tenv: Looks for environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY\n" +
	"\tfile: Uses the credentials file specified by the parameter aws-creds-file\n" +
	"\n" +
	"GCP remote urls should be of the form gs://gcs-bucket/database and will use the credentials setup using the gcloud " +
	"command line available from Google" +
	"\n" +
	"The local filesystem can be used as a remote by providing a repository url in the format file://absolute path. See" +
	"https://en.wikipedia.org/wiki/File_URI_scheme for details." +
	"\n<b>rename</b>\n" +
	"Rename the remote named <old> to <new>. All remote-tracking branches and configuration" +
	"settings for the remote are updated." +
	"\n" +
	"\n<b>remove, rm</b>\n" +
	"Remove the remote named <name>. All remote-tracking branches and configuration settings" +
	"for the remote are removed."

var remoteSynopsis = []string{
	"[-v | --verbose]",
	"add [--aws-region <region>] [--aws-creds-type <creds-type>] [--aws-creds-file <file>] [--aws-creds-profile <profile>] <name> <url>",
	"rename <old> <new>",
	"remove <name>",
}

const (
	addRemoteId    = "add"
	renameRemoteId = "rename"
	removeRemoteId = "remove"

	DolthubHostName = "dolthub.com"
)

var awsParams = []string{dbfactory.AWSRegionParam, dbfactory.AWSCredsTypeParam, dbfactory.AWSCredsFileParam, dbfactory.AWSCredsProfile}
var credTypes = []string{dbfactory.RoleCS.String(), dbfactory.EnvCS.String(), dbfactory.FileCS.String()}

func Remote(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["region"] = "cloud provider region associated with this remote."
	ap.ArgListHelp["creds-type"] = "credential type.  Valid options are role, env, and file.  See the help section for additional details."
	ap.ArgListHelp["profile"] = "AWS profile to use."
	ap.SupportsFlag(verboseFlag, "v", "When printing the list of remotes adds additional details.")
	ap.SupportsString(dbfactory.AWSRegionParam, "", "region", "")
	ap.SupportsValidatedString(dbfactory.AWSCredsTypeParam, "", "creds-type", "", argparser.ValidatorFromStrList(dbfactory.AWSCredsTypeParam, credTypes))
	ap.SupportsString(dbfactory.AWSCredsFileParam, "", "file", "AWS credentials file")
	ap.SupportsString(dbfactory.AWSCredsProfile, "", "profile", "AWS profile to use")
	help, usage := cli.HelpAndUsagePrinters(commandStr, remoteShortDesc, remoteLongDesc, remoteSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError

	switch {
	case apr.NArg() == 0:
		verr = printRemotes(dEnv, apr)
	case apr.Arg(0) == addRemoteId:
		verr = addRemote(dEnv, apr)
	case apr.Arg(0) == renameRemoteId:
		verr = renameRemote(dEnv, apr)
	case apr.Arg(0) == removeRemoteId:
		verr = removeRemote(dEnv, apr)
	default:
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func removeRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
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
	} else {
		delete(dEnv.RepoState.Remotes, old)
		err := dEnv.RepoState.Save()

		if err != nil {
			return errhand.BuildDError("error: unable to save changes.").AddCause(err).Build()
		}
	}

	return nil
}

func renameRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	old := strings.TrimSpace(apr.Arg(1))
	new := strings.TrimSpace(apr.Arg(2))

	remotes, err := dEnv.GetRemotes()

	if err != nil {
		return errhand.BuildDError("error: unable to read remotes").Build()
	}

	if r, ok := remotes[old]; !ok {
		return errhand.BuildDError("error: unknown remote " + old).Build()
	} else {
		delete(dEnv.RepoState.Remotes, old)

		r.Name = new
		dEnv.RepoState.AddRemote(r)

		err := dEnv.RepoState.Save()

		if err != nil {
			return errhand.BuildDError("error: unable to save changes.").AddCause(err).Build()
		}
	}

	return nil
}

func getAbsRemoteUrl(cfg config.ReadableConfig, urlArg string) (string, string, error) {
	u, err := earl.Parse(urlArg)

	if err != nil {
		return "", "", err
	}

	if u.Scheme != "" {
		return u.Scheme, urlArg, nil
	} else if u.Host != "" {
		return "https", "https://" + urlArg, nil
	}

	hostName, err := cfg.GetString(env.RemotesApiHostKey)

	if err != nil {
		if err != config.ErrConfigParamNotFound {
			return "", "", err
		}

		hostName = DolthubHostName
	}

	hostName = strings.TrimSpace(hostName)

	return "https", "https://" + path.Join(hostName, u.Path), nil
}

func addRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("").SetPrintUsage().Build()
	}

	remoteName := strings.TrimSpace(apr.Arg(1))

	if strings.IndexAny(remoteName, " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|") != -1 {
		return errhand.BuildDError("invalid remote name: " + remoteName).Build()
	}

	remoteUrl := apr.Arg(2)
	scheme, remoteUrl, err := getAbsRemoteUrl(dEnv.Config, remoteUrl)

	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", remoteUrl).Build()
	}

	params, verr := parseRemoteArgs(apr, scheme, remoteUrl)

	if verr != nil {
		return verr
	}

	r := env.NewRemote(remoteName, remoteUrl, params)
	dEnv.RepoState.AddRemote(r)
	err = dEnv.RepoState.Save()

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
