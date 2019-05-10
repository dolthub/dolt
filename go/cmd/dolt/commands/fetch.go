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
	"[<refspec> ...]",
}

func Fetch(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, fetchShortDesc, fetchLongDesc, fetchSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError
	refSpecs, verr := getRefSpecs(apr, dEnv)

	if verr == nil {
		var rsToRem map[ref.RemoteRefSpec]env.Remote
		if rsToRem, verr = mapRefspecsToRemotes(refSpecs, dEnv); verr == nil {
			verr = fetchRefSpecs(dEnv, rsToRem)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func getRefSpecs(apr *argparser.ArgParseResults, dEnv *env.DoltEnv) ([]ref.RemoteRefSpec, errhand.VerboseError) {
	if apr.NArg() != 0 {
		return parseRSFromArgs(apr)
	}

	return dEnv.GetRefSpecs("")
}

func parseRSFromArgs(apr *argparser.ArgParseResults) ([]ref.RemoteRefSpec, errhand.VerboseError) {
	var refSpecs []ref.RemoteRefSpec
	for i := 0; i < apr.NArg(); i++ {
		rsStr := apr.Arg(i)
		rs, err := ref.ParseRefSpec(rsStr)

		if err != nil {
			return nil, errhand.BuildDError("error: '%s' is not a valid refspec.", rsStr).SetPrintUsage().Build()
		}

		if rrs, ok := rs.(ref.RemoteRefSpec); !ok {
			return nil, errhand.BuildDError("error: '%s' is not a valid refspec referring to a remote tracking branch", rsStr).Build()
		} else {
			refSpecs = append(refSpecs, rrs)
		}
	}

	return refSpecs, nil
}

func mapRefspecsToRemotes(refSpecs []ref.RemoteRefSpec, dEnv *env.DoltEnv) (map[ref.RemoteRefSpec]env.Remote, errhand.VerboseError) {
	nameToRemote := dEnv.RepoState.Remotes

	rsToRem := make(map[ref.RemoteRefSpec]env.Remote)
	for _, rrs := range refSpecs {
		remName := rrs.GetRemote()

		if rem, ok := nameToRemote[remName]; !ok {
			return nil, errhand.BuildDError("error: unknown remote '%s'", remName).Build()
		} else {
			rsToRem[rrs] = rem
		}
	}

	return rsToRem, nil
}

func fetchRefSpecs(dEnv *env.DoltEnv, rsToRem map[ref.RemoteRefSpec]env.Remote) errhand.VerboseError {
	ctx := context.TODO()

	for rs, rem := range rsToRem {
		srcDB := rem.GetRemoteDB(ctx)

		branchRefs := srcDB.GetRefs(ctx)
		for _, branchRef := range branchRefs {
			remoteTrackRef := rs.DestRef(branchRef)

			if remoteTrackRef != nil {
				verr := fetchRemoteBranch(rem, srcDB, dEnv.DoltDB, branchRef, remoteTrackRef)

				if verr != nil {
					return verr
				}
			}
		}
	}

	return nil
}

func fetchRemoteBranch(rem env.Remote, srcDB, destDB *doltdb.DoltDB, srcRef, destRef ref.DoltRef) (verr errhand.VerboseError) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			verr = remotePanicRecover(r, stack)
		}
	}()

	cs, _ := doltdb.NewCommitSpec("HEAD", srcRef.String())
	cm, err := srcDB.Resolve(context.TODO(), cs)

	if err != nil {
		verr = errhand.BuildDError("error: unable to find '%s' on '%s'", srcRef.GetPath(), rem.Name).Build()
	} else {
		progChan := make(chan datas.PullProgress)
		stopChan := make(chan struct{})
		go progFunc(progChan, stopChan)

		err = actions.Fetch(context.TODO(), destRef, srcDB, destDB, cm, progChan)

		close(progChan)
		<-stopChan

		if err != nil {
			verr = errhand.BuildDError("error: fetch failed").AddCause(err).Build()
		}
	}

	return
}
