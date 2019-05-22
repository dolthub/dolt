package commands

import (
	"errors"
	"fmt"
	"path"
	"strconv"
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
	"\nadd\n" +
	"Adds a remote named <name> for the repository at <url>. The command git fetch <name> can" +
	"then be used to create and update remote-tracking branches <name>/<branch>." +
	"\n" +
	"\nrename\n" +
	"Rename the remote named <old> to <new>. All remote-tracking branches and configuration" +
	"settings for the remote are updated." +
	"\n" +
	"\nremove, rm\n" +
	"Remove the remote named <name>. All remote-tracking branches and configuration settings" +
	"for the remote are removed."

var remoteSynopsis = []string{
	"[-v | --verbose]",
	"add [-insecure] <name> <url>",
	"rename <old> <new>",
	"remove <name>",
}

const (
	addRemoteId    = "add"
	renameRemoteId = "rename"
	removeRemoteId = "remove"

	DolthubHostName = "dolthub.com"
)

func Remote(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(verboseFlag, "v", "When printing the list of remotes adds additional details.")
	ap.SupportsFlag(insecureFlag, "", "Use an unencrypted connection.")
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

func getAbsRemoteUrl(cfg config.ReadableConfig, urlArg string) (string, error) {
	u, err := earl.Parse(urlArg)

	if err != nil {
		return "", err
	}

	if u.Scheme != "" || u.Host != "" {
		return urlArg, nil
	}

	hostName, err := cfg.GetString(env.RemotesApiHostKey)

	if err != nil {
		if err != config.ErrConfigParamNotFound {
			return "", err
		}

		hostName = DolthubHostName
	}

	hostName = strings.TrimSpace(hostName)

	portStr, err := cfg.GetString(env.RemotesApiHostPortKey)

	if err != nil {
		if err != config.ErrConfigParamNotFound {
			return "", err
		}

		portStr = "443"
	}

	portStr = strings.TrimSpace(portStr)
	portNum, err := strconv.ParseUint(portStr, 10, 16)

	if err != nil {
		return "", ErrInvalidPort
	}

	return path.Join(fmt.Sprintf("%s:%d", hostName, portNum), u.Path), nil
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
	remoteUrl, err := getAbsRemoteUrl(dEnv.Config, remoteUrl)

	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", remoteUrl).Build()
	}

	r := env.NewRemote(remoteName, remoteUrl, apr.Contains(insecureFlag))
	dEnv.RepoState.AddRemote(r)
	err = dEnv.RepoState.Save()

	if err != nil {
		return errhand.BuildDError("error: Unable to save changes.").AddCause(err).Build()
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
			secureStr := "secure"
			if env.IsInsecure(r) {
				secureStr = "insecure"
			}

			cli.Printf("%s %s %s\n", r.Name, r.Url, secureStr)
		} else {
			cli.Println(r.Name)
		}
	}

	return nil
}
