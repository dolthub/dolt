package commands

import (
	"fmt"
	"github.com/attic-labs/noms/go/datas"
	"github.com/dustin/go-humanize"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"time"
)

var pushShortDesc = ""
var pushLongDesc = ""
var pushSynopsis = []string{
	"<remote> <branch>",
}

func Push(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, pushShortDesc, pushLongDesc, pushSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError
	if apr.NArg() != 2 {
		verr = errhand.BuildDError("Invalid usage").SetPrintUsage().Build()
	} else {
		remoteName := apr.Arg(0)
		branch := apr.Arg(1)

		remotes, err := dEnv.GetRemotes()

		if err != nil {
			verr = errhand.BuildDError("error: failed to read remotes from config.").Build()
		}

		if remote, ok := remotes[remoteName]; !ok {
			verr = errhand.BuildDError("fatal: unknown remote " + remoteName).Build()
		} else if !dEnv.DoltDB.HasBranch(branch) {
			verr = errhand.BuildDError("fatal: unknown branch " + branch).Build()
		} else {
			verr = pushToRemoteBranch(dEnv, remote, branch)
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func pushToRemoteBranch(dEnv *env.DoltEnv, r *env.Remote, branch string) errhand.VerboseError {
	cs, _ := doltdb.NewCommitSpec("HEAD", branch)
	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return errhand.BuildDError("error: unable to find", branch).Build()
	}

	destDB := r.GetRemoteDB()

	progChan := make(chan datas.PullProgress, 16)
	stopChan := make(chan struct{})
	go progFunc(progChan, stopChan)

	remoteBranch := doltdb.LongRemoteBranchName(r.Name, branch)
	err = actions.Push(branch, remoteBranch, dEnv.DoltDB, destDB, cm, progChan)
	close(progChan)
	<-stopChan

	if err != nil {
		if err == doltdb.ErrUpToDate {
			cli.Println("Everything up-to-date")
		} else if err == doltdb.ErrIsAhead || err == actions.ErrCantFF || err == datas.ErrMergeNeeded {
			cli.Printf("To %s\n", r.Url)
			cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", branch, remoteBranch)
			cli.Printf("error: failed to push some refs to '%s'\n", r.Url)
			cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
			cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
			cli.Println("hint: 'dolt pull ...') before pushing again.")
		} else {
			return errhand.BuildDError("error: push failed").AddCause(err).Build()
		}
	}

	return nil
}

func progFunc(progChan chan datas.PullProgress, stopChan chan struct{}) {
	var latest datas.PullProgress
	last := time.Now().UnixNano() - 1
	lenPrinted := 0
	done := false
	for !done {
		select {
		case progress, ok := <-progChan:
			if !ok {
				done = true
			}
			latest = progress

		case <-time.After(250 * time.Millisecond):
			break
		}

		nowUnix := time.Now().UnixNano()
		deltaTime := time.Duration(nowUnix - last)
		halfSec := 500 * time.Millisecond
		if done || deltaTime > halfSec {
			last = nowUnix
			if latest.KnownCount > 0 {
				progMsg := fmt.Sprintf("Counted chunks: %d, Buffered chunks: %d)", latest.KnownCount, latest.DoneCount)
				lenPrinted = cli.DeleteAndPrint(lenPrinted, progMsg)
			}
		}
	}

	if lenPrinted > 0 {
		cli.Println()
	}
	stopChan <- struct{}{}
}

func bytesPerSec(bytes uint64, start time.Time) string {
	bps := float64(bytes) / float64(time.Since(start).Seconds())
	return humanize.Bytes(uint64(bps))
}
