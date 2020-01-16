// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"sync"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/remotestorage"
	"github.com/liquidata-inc/dolt/go/libraries/events"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/earl"
	"github.com/liquidata-inc/dolt/go/store/datas"
)

const (
	SetUpstreamFlag = "set-upstream"
)

var pushShortDesc = "Update remote refs along with associated objects"

var pushLongDesc = "Updates remote refs using local refs, while sending objects necessary to complete the given refs." +
	"\n" +
	"\nWhen the command line does not specify where to push with the <remote> argument, an attempt is made to infer the " +
	"remote.  If only one remote exists it will be used, if multiple remotes exists, a remote named 'origin' will be " +
	"attempted.  If there is more than one remote, and none of them are named 'origin' then the command will fail and " +
	"you will need to specify the correct remote explicitly." +
	"\n" +
	"\nWhen the command line does not specify what to push with <refspec>... then the current branch will be used." +
	"\n" +
	"\nWhen neither the command-line does not specify what to push, the default behavior is used, which corresponds to the " +
	"current branch being pushed to the corresponding upstream branch, but as a safety measure, the push is aborted if " +
	"the upstream branch does not have the same name as the local one."

var pushSynopsis = []string{
	"[-u | --set-upstream] [<remote>] [<refspec>]",
}

type PushCmd struct{}

func (cmd PushCmd) Name() string {
	return "push"
}

func (cmd PushCmd) Description() string {
	return "Push to a dolt remote."
}

func (cmd PushCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, pushShortDesc, pushLongDesc, pushSynopsis, ap)
}

func (cmd PushCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(SetUpstreamFlag, "u", "For every branch that is up to date or successfully pushed, add upstream (tracking) reference, used by argument-less dolt pull and other commands.")
	return ap
}

func (cmd PushCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PUSH
}

func (cmd PushCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, pushShortDesc, pushLongDesc, pushSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	remotes, err := dEnv.GetRemotes()

	if err != nil {
		cli.PrintErrln("error: failed to read remotes from config.")
		return 1
	}

	remoteName := "origin"
	remote, remoteOK := remotes[remoteName]

	currentBranch := dEnv.RepoState.Head.Ref
	upstream, hasUpstream := dEnv.RepoState.Branches[currentBranch.GetPath()]

	var refSpec ref.RefSpec
	var verr errhand.VerboseError
	if remoteOK && apr.NArg() == 1 {
		refSpecStr := apr.Arg(0)
		refSpec, err = ref.ParseRefSpec(refSpecStr)

		if err != nil {
			verr = errhand.BuildDError("error: invalid refspec '%s'", refSpecStr).AddCause(err).Build()
		}
	} else if apr.NArg() == 2 {
		remoteName = apr.Arg(0)
		refSpecStr := apr.Arg(1)
		refSpec, err = ref.ParseRefSpec(refSpecStr)

		if err != nil {
			verr = errhand.BuildDError("error: invalid refspec '%s'", refSpecStr).AddCause(err).Build()
		}
	} else if apr.Contains(SetUpstreamFlag) {
		verr = errhand.BuildDError("error: --set-upstream requires <remote> and <refspec> params.").SetPrintUsage().Build()
	} else if hasUpstream {
		if apr.NArg() > 0 {
			cli.PrintErrf("fatal: upstream branch set for '%s'.  Use 'dolt push' without arguments to push.\n", currentBranch)
			return 1
		}

		if currentBranch.GetPath() != upstream.Merge.Ref.GetPath() {
			cli.PrintErrln("fatal: The upstream branch of your current branch does not match")
			cli.PrintErrln("the name of your current branch.  To push to the upstream branch")
			cli.PrintErrln("on the remote, use")
			cli.PrintErrln()
			cli.PrintErrln("\tdolt push origin HEAD:" + currentBranch.GetPath())
			cli.PrintErrln()
			cli.PrintErrln("To push to the branch of the same name on the remote, use")
			cli.PrintErrln()
			cli.PrintErrln("\tdolt push origin HEAD")
			return 1
		}

		remoteName = upstream.Remote
		refSpec, _ = ref.NewBranchToBranchRefSpec(currentBranch.(ref.BranchRef), upstream.Merge.Ref.(ref.BranchRef))
	} else {
		if apr.NArg() == 0 {
			remoteName = "<remote>"
			if defRemote, verr := dEnv.GetDefaultRemote(); verr == nil {
				remoteName = defRemote.Name
			}

			cli.PrintErrln("fatal: The current branch " + currentBranch.GetPath() + " has no upstream branch.")
			cli.PrintErrln("To push the current branch and set the remote as upstream, use")
			cli.PrintErrln()
			cli.PrintErrln("\tdolt push --set-upstream " + remoteName + " " + currentBranch.GetPath())
			return 1
		}

		verr = errhand.BuildDError("").SetPrintUsage().Build()
	}

	remote, remoteOK = remotes[remoteName]

	if !remoteOK {
		cli.PrintErrln("fatal: unknown remote " + remoteName)
		return 1
	}

	if verr == nil {
		hasRef, err := dEnv.DoltDB.HasRef(ctx, currentBranch)

		if err != nil {
			verr = errhand.BuildDError("error: failed to read from db").AddCause(err).Build()
		} else if !hasRef {
			verr = errhand.BuildDError("fatal: unknown branch " + currentBranch.GetPath()).Build()
		} else {
			src := refSpec.SrcRef(currentBranch)
			dest := refSpec.DestRef(src)

			var remoteRef ref.DoltRef
			remoteRef, verr = getTrackingRef(dest, remote)

			if verr == nil {
				destDB, err := remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

				if err != nil {
					bdr := errhand.BuildDError("error: failed to get remote db").AddCause(err)

					if err == remotestorage.ErrInvalidDoltSpecPath {
						urlObj, _ := earl.Parse(remote.Url)
						bdr.AddDetails("For the remote: %s %s", remote.Name, remote.Url)

						path := urlObj.Path
						if path[0] == '/' {
							path = path[1:]
						}

						bdr.AddDetails("'%s' should be in the format 'organization/repo'", path)
					}

					verr = bdr.Build()
				} else if src == ref.EmptyBranchRef {
					verr = deleteRemoteBranch(ctx, dest, remoteRef, dEnv.DoltDB, destDB, remote)
				} else {
					verr = pushToRemoteBranch(ctx, dEnv, src, dest, remoteRef, dEnv.DoltDB, destDB, remote)
				}
			}

			if verr == nil && apr.Contains(SetUpstreamFlag) {
				if dEnv.RepoState.Branches == nil {
					dEnv.RepoState.Branches = map[string]env.BranchConfig{}
				}

				dEnv.RepoState.Branches[src.GetPath()] = env.BranchConfig{
					Merge:  ref.MarshalableRef{Ref: dest},
					Remote: remoteName,
				}

				err := dEnv.RepoState.Save(dEnv.FS)

				if err != nil {
					verr = errhand.BuildDError("error: failed to save repo state").AddCause(err).Build()
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

func pushToRemoteBranch(ctx context.Context, dEnv *env.DoltEnv, srcRef, destRef, remoteRef ref.DoltRef, localDB, remoteDB *doltdb.DoltDB, remote env.Remote) errhand.VerboseError {
	evt := events.GetEventFromContext(ctx)

	u, err := earl.Parse(remote.Url)

	if err == nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	cs, _ := doltdb.NewCommitSpec("HEAD", srcRef.GetPath())
	cm, err := localDB.Resolve(ctx, cs)

	if err != nil {
		return errhand.BuildDError("error: unable to find %v", srcRef.GetPath()).Build()
	} else {
		wg, progChan, pullerEventCh := runProgFuncs()
		err = actions.Push(ctx, dEnv, destRef.(ref.BranchRef), remoteRef.(ref.RemoteRef), localDB, remoteDB, cm, progChan, pullerEventCh)
		stopProgFuncs(wg, progChan, pullerEventCh)

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
				return errhand.BuildDError("error: push failed").AddCause(err).Build()
			}
		}
	}

	return nil
}

func pullerProgFunc(pullerEventCh chan datas.PullerEvent) {
	var pos int
	for evt := range pullerEventCh {
		switch evt.EventType {
		case datas.NewLevelTWEvent:
			if evt.TWEventDetails.TreeLevel == -1 {
				continue
			}

			pos = cli.DeleteAndPrint(0, fmt.Sprintf("Tree Level: %d has %d new chunks. Determining how many are needed.", evt.TWEventDetails.TreeLevel, evt.TWEventDetails.ChunksInLevel))
		case datas.DestDBHasTWEvent:
			if evt.TWEventDetails.TreeLevel == -1 {
				continue
			}

			cli.DeleteAndPrint(pos, fmt.Sprintf("Tree Level: %d has %d new chunks of which %d already exist in the database. Buffering %d chunks.\n", evt.TWEventDetails.TreeLevel, evt.TWEventDetails.ChunksInLevel, evt.TWEventDetails.ChunksAlreadyHad, evt.TWEventDetails.ChunksInLevel-evt.TWEventDetails.ChunksAlreadyHad))
			pos = 0

		case datas.LevelUpdateTWEvent:
			if evt.TWEventDetails.TreeLevel == -1 {
				continue
			}

			toBuffer := evt.TWEventDetails.ChunksInLevel - evt.TWEventDetails.ChunksAlreadyHad

			var percentBuffered float64
			if toBuffer > 0 {
				percentBuffered = 100 * float64(evt.TWEventDetails.ChunksBuffered) / float64(toBuffer)
			}

			pos = cli.DeleteAndPrint(pos, fmt.Sprintf("Tree Level: %d. %.2f%% of new chunks buffered.", evt.TWEventDetails.TreeLevel, percentBuffered))

		case datas.LevelDoneTWEvent:
			if evt.TWEventDetails.TreeLevel == -1 {
				continue
			}

			_ = cli.DeleteAndPrint(pos, fmt.Sprintf("Tree Level: %d. %.2f%% of new chunks buffered.", evt.TWEventDetails.TreeLevel, 100.0))

			pos = 0
			cli.Println("")

		case datas.StartUploadTableFile:
			pos = cli.DeleteAndPrint(pos, fmt.Sprintf("Uploading file %d of %d. File size: %s.", evt.TFEventDetails.TableFilesUploaded+1, evt.TFEventDetails.TableFileCount, humanize.Bytes(uint64(evt.TFEventDetails.CurrentFileSize))))

		case datas.EndUpdateTableFile:
			pos = cli.DeleteAndPrint(pos, fmt.Sprintf("Successfully uploaded %d of %d file(s).", evt.TFEventDetails.TableFilesUploaded, evt.TFEventDetails.TableFileCount))
		}
	}
}

func progFunc(progChan chan datas.PullProgress) {
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
}

func runProgFuncs() (*sync.WaitGroup, chan datas.PullProgress, chan datas.PullerEvent) {
	pullerEventCh := make(chan datas.PullerEvent, 128)
	progChan := make(chan datas.PullProgress, 128)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		progFunc(progChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pullerProgFunc(pullerEventCh)
	}()

	return wg, progChan, pullerEventCh
}

func stopProgFuncs(wg *sync.WaitGroup, progChan chan datas.PullProgress, pullerEventCh chan datas.PullerEvent) {
	close(progChan)
	close(pullerEventCh)
	wg.Wait()

	cli.Println()
}

func bytesPerSec(bytes uint64, start time.Time) string {
	bps := float64(bytes) / float64(time.Since(start).Seconds())
	return humanize.Bytes(uint64(bps))
}
