package commands

import (
	"github.com/attic-labs/noms/go/datas"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"path"
)

var fetchShortDesc = ""
var fetchLongDesc = ""
var fetchSynopsis = []string{
	"<remote> <branch>",
}

func Fetch(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, loginShortDesc, loginLongDesc, loginSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError
	if apr.NArg() != 2 {
		verr = errhand.BuildDError("Invalid usage").SetPrintUsage().Build()
	} else {
		remoteName := apr.Arg(0)
		branch := apr.Arg(1)

		remotes, err := dEnv.GetRemotes()

		if err != nil {
			verr = errhand.BuildDError("error: fetch failed").AddCause(err).Build()
		} else if remote, ok := remotes[remoteName]; !ok {
			verr = errhand.BuildDError("fatal: unknown remote " + remoteName).Build()
		} else {
			verr = fetchRemoteBranch(dEnv, remote, remoteName, branch)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func fetchRemoteBranch(dEnv *env.DoltEnv, r *env.Remote, remoteName, branch string) errhand.VerboseError {
	srcDB := r.GetRemoteDB()

	if !srcDB.HasBranch(branch) {
		return errhand.BuildDError("fatal: unknown branch " + branch).Build()
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", branch)
	cm, err := srcDB.Resolve(cs)

	if err != nil {
		return errhand.BuildDError("error: unable to find", branch).Build()
	}

	progChan := make(chan datas.PullProgress)
	stopChan := make(chan struct{})
	go progFunc(progChan, stopChan)

	err = actions.Fetch(path.Join("remotes", remoteName, branch), srcDB, dEnv.DoltDB, cm, progChan)
	close(progChan)
	<-stopChan

	if err != nil {
		return errhand.BuildDError("error: fetch failed").AddCause(err).Build()
	}

	return nil
}
