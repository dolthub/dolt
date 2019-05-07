package commands

import (
	"context"
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
	branch := dEnv.RepoState.Head.Ref

	var verr errhand.VerboseError
	var remoteName string
	if apr.NArg() > 1 {
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	} else {
		if apr.NArg() == 1 {
			remoteName = apr.Arg(0)
		}

		var refSpecs []ref.RemoteRefSpec
		refSpecs, verr = dEnv.GetRefSpecs(remoteName)

		if verr == nil {
			if len(refSpecs) == 0 {
				verr = errhand.BuildDError("error: no refspec for remote").Build()
			} else {
				remote := dEnv.RepoState.Remotes[refSpecs[0].GetRemote()]

				for _, refSpec := range refSpecs {
					if remoteTrackRef := refSpec.Map(branch); remoteTrackRef != nil {
						verr = pullRemoteBranch(dEnv, remote, branch, remoteTrackRef)

						if verr != nil {
							break
						}
					}
				}
			}
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func pullRemoteBranch(dEnv *env.DoltEnv, r env.Remote, srcRef, destRef ref.DoltRef) (verr errhand.VerboseError) {
	srcDB := r.GetRemoteDB(context.TODO())
	verr = fetchRemoteBranch(r, srcDB, dEnv.DoltDB, srcRef, destRef)

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
