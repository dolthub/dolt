package commands

import (
	"path"
	"runtime/debug"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var pullShortDesc = ""
var pullLongDesc = ""
var pullSynopsis = []string{
	"<remote>",
}

func Pull(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, pullShortDesc, pullLongDesc, pullSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	branch := dEnv.RepoState.Branch

	var verr errhand.VerboseError
	if apr.NArg() != 1 {
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	} else {
		remoteName := apr.Arg(0)

		remotes, err := dEnv.GetRemotes()

		if err != nil {
			verr = errhand.BuildDError("error: pull failed").AddCause(err).Build()
		} else if remote, ok := remotes[remoteName]; !ok {
			verr = errhand.BuildDError("fatal: unknown remote " + remoteName).Build()
		} else {
			verr = pullRemoteBranch(dEnv, remote, remoteName, branch)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func pullRemoteBranch(dEnv *env.DoltEnv, r env.Remote, remoteName, branch string) (verr errhand.VerboseError) {
	verr = fetchRemoteBranch(dEnv, r, remoteName, branch)

	if verr == nil {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				verr = remotePanicRecover(r, stack)
			}
		}()

		mergeBranch(dEnv, path.Join("remotes", remoteName, branch))
	}

	return
}
