package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"strings"
)

var remoteShortDesc = ""
var remoteLongDesc = ""
var remoteSynopsis = []string{
	"[-v | --verbose]",
	"add <name> <url>",
	"rename <old> <new>",
	"remove <name>",
}

const (
	addRemoteId    = "add"
	renameRemoteId = "rename"
	removeRemoteId = "remove"
)

func Remote(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(verboseFlag, "v", "")
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
		verr = errhand.BuildDError("Invalid Usage").SetPrintUsage().Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}

func removeRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 2 {
		return errhand.BuildDError("Invalid usage.").SetPrintUsage().Build()
	}

	old := strings.TrimSpace(apr.Arg(1))
	cfg, _ := dEnv.Config.GetConfig(env.LocalConfig)

	oldId := "remote." + old + ".url"

	if _, err := cfg.GetString(oldId); err != nil {
		return errhand.BuildDError("error: unknown remote " + oldId).Build()
	} else {
		cfg.Unset([]string{oldId})
	}

	return nil
}

func renameRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("Invalid usage.").SetPrintUsage().Build()
	}

	old := strings.TrimSpace(apr.Arg(1))
	new := strings.TrimSpace(apr.Arg(2))
	cfg, _ := dEnv.Config.GetConfig(env.LocalConfig)

	oldId := env.RemoteConfigParam(old, env.RemoteUrlParam)
	newId := env.RemoteConfigParam(new, env.RemoteUrlParam)

	if val, err := cfg.GetString(oldId); err != nil {
		return errhand.BuildDError("error: unknown remote " + oldId).Build()
	} else {
		cfg.Unset([]string{oldId})
		cfg.SetStrings(map[string]string{newId: val})
	}

	return nil
}

func addRemote(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 3 {
		return errhand.BuildDError("Invalid usage.").SetPrintUsage().Build()
	}

	remoteName := strings.TrimSpace(apr.Arg(1))
	remoteUrl := apr.Arg(2)

	if strings.IndexAny(remoteName, " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|") != -1 {
		return errhand.BuildDError("invalid remote name: " + remoteName).Build()
	}

	cfg, _ := dEnv.Config.GetConfig(env.LocalConfig)
	key := env.RemoteConfigParam(remoteName, env.RemoteUrlParam)

	cfg.SetStrings(map[string]string{key: remoteUrl})

	return nil
}

func printRemotes(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	remotes, err := dEnv.GetRemotes()

	if err != nil {
		return errhand.BuildDError("Unable to get remotes from the local directory").AddCause(err).Build()
	}

	for _, remote := range remotes {
		if apr.Contains(verboseFlag) {
			cli.Printf("%s %s\n", remote.Name, remote.Url)
		} else {
			cli.Println(remote.Name)
		}
	}

	return nil
}
