package commands

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"runtime/debug"

	"github.com/attic-labs/noms/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var fetchShortDesc = ""
var fetchLongDesc = ""
var fetchSynopsis = []string{
	"<remote> <branch>",
}

func Fetch(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, fetchShortDesc, fetchLongDesc, fetchSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError
	if apr.NArg() != 2 {
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	} else {
		remoteName := apr.Arg(0)
		branch := apr.Arg(1)

		remotes, err := dEnv.GetRemotes()

		if err != nil {
			verr = errhand.BuildDError("error: fetch failed").AddCause(err).Build()
		} else if remote, ok := remotes[remoteName]; !ok {
			verr = errhand.BuildDError("fatal: unknown remote " + remoteName).Build()
		} else {
			remoteRef := ref.NewRemoteRef(remoteName, branch)
			localRef := ref.NewBranchRef(branch)
			verr = fetchRemoteBranch(dEnv, remote, localRef, remoteRef)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func fetchRemoteBranch(dEnv *env.DoltEnv, r env.Remote, srcRef, destRef ref.DoltRef) (verr errhand.VerboseError) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			verr = remotePanicRecover(r, stack)
		}
	}()

	srcDB := r.GetRemoteDB(context.TODO())

	if !srcDB.HasRef(context.TODO(), srcRef) {
		verr = errhand.BuildDError("fatal: unknown branch " + srcRef.String()).Build()
	} else {
		cs, _ := doltdb.NewCommitSpec("HEAD", srcRef.String())
		cm, err := srcDB.Resolve(context.TODO(), cs)

		if err != nil {
			verr = errhand.BuildDError("error: unable to find %v", srcRef.Path).Build()
		} else {
			progChan := make(chan datas.PullProgress)
			stopChan := make(chan struct{})
			go progFunc(progChan, stopChan)

			err = actions.Fetch(context.TODO(), destRef, srcDB, dEnv.DoltDB, cm, progChan)

			close(progChan)
			<-stopChan

			if err != nil {
				verr = errhand.BuildDError("error: fetch failed").AddCause(err).Build()
			}
		}
	}

	return
}
