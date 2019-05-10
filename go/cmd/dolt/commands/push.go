package commands

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/ref"
	"runtime/debug"
	"time"

	"github.com/attic-labs/noms/go/datas"
	"github.com/dustin/go-humanize"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var pushShortDesc = ""
var pushLongDesc = ""
var pushSynopsis = []string{
	"<remote> <branch>",
}

func Push(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag("set-upstream", "u", "For every branch that is up to date or successfully pushed, add upstream (tracking) reference, used by argument-less <b>dolt pull</b> and other commands.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, pushShortDesc, pushLongDesc, pushSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	currentBranch := dEnv.RepoState.Head.Ref
	upstream, hasUpstream := dEnv.RepoState.Branches[currentBranch.GetPath()]

	var verr errhand.VerboseError
	var remoteName string
	var refSpec ref.RefSpec
	if !hasUpstream && apr.NArg() == 0 {
		remoteName = "<remote>"
		if defRemote, verr := dEnv.GetDefaultRemote(); verr == nil {
			remoteName = defRemote.Name
		}

		cli.Println("fatal: The current branch " + currentBranch.GetPath() + " has no upstream branch.")
		cli.Println("To push the current branch and set the remote as upstream, use")
		cli.Println()
		cli.Println("\tdolt push --set-upstream " + remoteName + " " + currentBranch.GetPath())
		return 1
	} else if !hasUpstream && apr.NArg() != 2 {
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	} else if hasUpstream && apr.NArg() == 0 {
		if currentBranch.GetPath() != upstream.Merge.Ref.GetPath() {
			cli.Println("fatal: The upstream branch of your current branch does not match")
			cli.Println("the name of your current branch.  To push to the upstream branch")
			cli.Println("on the remote, use")
			cli.Println()
			cli.Println("\tdolt push origin HEAD:" + currentBranch.GetPath())
			cli.Println()
			cli.Println("To push to the branch of the same name on the remote, use")
			cli.Println()
			cli.Println("\tdolt push origin HEAD")
			return 1
		} else {
			remoteName = upstream.Remote
			refSpec, _ = ref.NewBranchToBranchRefSpec(currentBranch.(ref.BranchRef), upstream.Merge.Ref.(ref.BranchRef))
		}
	} else {
		remoteName = apr.Arg(0)
		refSpecStr := apr.Arg(1)

		var err error
		refSpec, err = ref.ParseRefSpec(refSpecStr)

		if err != nil {
			verr = errhand.BuildDError("error: '%s' is not a valid refspec.", refSpecStr).SetPrintUsage().Build()
		}
	}

	if verr == nil {
		remotes, err := dEnv.GetRemotes()

		if err != nil {
			verr = errhand.BuildDError("error: failed to read remotes from config.").Build()
		}

		if remote, ok := remotes[remoteName]; !ok {
			verr = errhand.BuildDError("fatal: unknown remote " + remoteName).Build()
		} else if !dEnv.DoltDB.HasRef(context.TODO(), currentBranch) {
			verr = errhand.BuildDError("fatal: unknown branch " + currentBranch.GetPath()).Build()
		} else {
			ctx := context.Background()
			src := refSpec.SrcRef(currentBranch)
			dest := refSpec.DestRef(src)

			var remoteRef ref.DoltRef
			remoteRef, verr = getTrackingRef(dest, remote)

			if verr == nil {
				destDB := remote.GetRemoteDB(ctx)

				if src == ref.EmptyBranchRef {
					verr = deleteRemoteBranch(ctx, dest, remoteRef, dEnv.DoltDB, destDB, remote)
				} else {
					verr = pushToRemoteBranch(ctx, src, dest, remoteRef, dEnv.DoltDB, destDB, remote)
				}
			}
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func getTrackingRef(branchRef ref.DoltRef, remote env.Remote) (ref.DoltRef, errhand.VerboseError) {
	for _, fsStr := range remote.FetchSpecs {
		fs, err := ref.ParseRefSpecForRemote(remote.Name, fsStr)

		if err != nil {
			return nil, errhand.BuildDError("error: invalid fetch spec '%s' for remote '%s'", fsStr, remote.Name).Build()
		}

		remoteRef := fs.DestRef(branchRef)

		if remoteRef != nil {
			return remoteRef, nil
		}
	}

	return nil, nil
}

func deleteRemoteBranch(ctx context.Context, toDelete, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote) errhand.VerboseError {
	err := actions.DeleteRemoteBranch(ctx, toDelete.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB)

	if err != nil {
		return errhand.BuildDError("error: failed to delete '%s' from remote '%s'", toDelete.String(), remote.Name).Build()
	}

	return nil
}

func pushToRemoteBranch(ctx context.Context, srcRef, destRef, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote) (verr errhand.VerboseError) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			verr = remotePanicRecover(r, stack)
		}
	}()

	cs, _ := doltdb.NewCommitSpec("HEAD", srcRef.GetPath())
	cm, err := localDB.Resolve(ctx, cs)

	if err != nil {
		verr = errhand.BuildDError("error: unable to find %v", srcRef.GetPath()).Build()
	} else {
		progChan := make(chan datas.PullProgress, 16)
		stopChan := make(chan struct{})
		go progFunc(progChan, stopChan)

		err = actions.Push(ctx, destRef.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, cm, progChan)

		close(progChan)
		<-stopChan

		if err != nil {
			if err == doltdb.ErrUpToDate {
				cli.Println("Everything up-to-date")
			} else if err == doltdb.ErrIsAhead || err == actions.ErrCantFF || err == datas.ErrMergeNeeded {
				cli.Printf("To %s\n", remote.Url)
				cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", destRef.String(), remoteRef.String())
				cli.Printf("error: failed to push some refs to '%s'\n", remote.Url)
				cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
				cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
				cli.Println("hint: 'dolt pull ...') before pushing again.")
			} else {
				verr = errhand.BuildDError("error: push failed").AddCause(err).Build()
			}
		}
	}

	return
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
