package commands

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
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
	branch := dEnv.RepoState.Head

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
			remoteRef := ref.NewRemoteRef(remoteName, branch.Path)
			verr = pullRemoteBranch(dEnv, remote, branch, remoteRef)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func pullRemoteBranch(dEnv *env.DoltEnv, r env.Remote, srcRef, destRef ref.DoltRef) (verr errhand.VerboseError) {
	verr = fetchRemoteBranch(dEnv, r, srcRef, destRef)

	if verr == nil {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				verr = remotePanicRecover(r, stack)
			}
		}()

		mergeBranch(dEnv, destRef)
	}

	return
}
