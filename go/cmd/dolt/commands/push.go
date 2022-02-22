// Copyright 2019 Dolthub, Inc.
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
	"io"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

var pushDocs = cli.CommandDocumentationContent{
	ShortDesc: "Update remote refs along with associated objects",
	LongDesc: `Updates remote refs using local refs, while sending objects necessary to complete the given refs.

When the command line does not specify where to push with the {{.LessThan}}remote{{.GreaterThan}} argument, an attempt is made to infer the remote.  If only one remote exists it will be used, if multiple remotes exists, a remote named 'origin' will be attempted.  If there is more than one remote, and none of them are named 'origin' then the command will fail and you will need to specify the correct remote explicitly.

When the command line does not specify what to push with {{.LessThan}}refspec{{.GreaterThan}}... then the current branch will be used.

When neither the command-line does not specify what to push, the default behavior is used, which corresponds to the current branch being pushed to the corresponding upstream branch, but as a safety measure, the push is aborted if the upstream branch does not have the same name as the local one.
`,

	Synopsis: []string{
		"[-u | --set-upstream] [{{.LessThan}}remote{{.GreaterThan}}] [{{.LessThan}}refspec{{.GreaterThan}}]",
	},
}

type PushCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd PushCmd) Name() string {
	return "push"
}

// Description returns a description of the command
func (cmd PushCmd) Description() string {
	return "Push to a dolt remote."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd PushCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, pushDocs, ap))
}

func (cmd PushCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(cli.SetUpstreamFlag, "u", "For every branch that is up to date or successfully pushed, add upstream (tracking) reference, used by argument-less {{.EmphasisLeft}}dolt pull{{.EmphasisRight}} and other commands.")
	ap.SupportsFlag(cli.ForceFlag, "f", "Update the remote with local history, overwriting any conflicting history in the remote.")
	return ap
}

// EventType returns the type of the event to log
func (cmd PushCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PUSH
}

// Exec executes the command
func (cmd PushCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, pushDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	opts, err := env.NewPushOpts(ctx, apr, dEnv.RepoStateReader(), dEnv.DoltDB, apr.Contains(cli.ForceFlag), apr.Contains(cli.SetUpstreamFlag))
	if err != nil {
		var verr errhand.VerboseError
		switch err {
		case env.ErrNoUpstreamForBranch:
			currentBranch := dEnv.RepoStateReader().CWBHeadRef()
			remoteName := "<remote>"
			if defRemote, verr := env.GetDefaultRemote(dEnv.RepoStateReader()); verr == nil {
				remoteName = defRemote.Name
			}
			verr = errhand.BuildDError("fatal: The current branch " + currentBranch.GetPath() + " has no upstream branch.\n" +
				"To push the current branch and set the remote as upstream, use\n" +
				"\tdolt push --set-upstream " + remoteName + " " + currentBranch.GetPath()).Build()
		case env.ErrInvalidSetUpstreamArgs:
			verr = errhand.BuildDError("error: --set-upstream requires <remote> and <refspec> params.").SetPrintUsage().Build()
		default:
			verr = errhand.VerboseErrorFromError(err)
		}
		return HandleVErrAndExitCode(verr, usage)
	}

	var verr errhand.VerboseError
	err = actions.DoPush(ctx, dEnv.RepoStateReader(), dEnv.RepoStateWriter(), dEnv.DoltDB, dEnv.TempTableFilesDir(), opts, buildProgStarter(defaultLanguage), stopProgFuncs)
	if err != nil {
		verr = printInfoForPushError(err, opts.Remote, opts.DestRef, opts.RemoteRef)
	}

	if opts.SetUpstream {
		err := dEnv.RepoState.Save(dEnv.FS)
		if err != nil {
			err = fmt.Errorf("%w; %s", actions.ErrFailedToSaveRepoState, err.Error())
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

const minUpdate = 100 * time.Millisecond

var spinnerSeq = []rune{'|', '/', '-', '\\'}

type TextSpinner struct {
	seqPos     int
	lastUpdate time.Time
}

func printInfoForPushError(err error, remote env.Remote, destRef, remoteRef ref.DoltRef) errhand.VerboseError {
	switch err {
	case doltdb.ErrUpToDate:
		cli.Println("Everything up-to-date")
	case doltdb.ErrIsAhead, actions.ErrCantFF, datas.ErrMergeNeeded:
		cli.Printf("To %s\n", remote.Url)
		cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", destRef.String(), remoteRef.String())
		cli.Printf("error: failed to push some refs to '%s'\n", remote.Url)
		cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
		cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
		cli.Println("hint: 'dolt pull ...') before pushing again.")
		return errhand.BuildDError("").Build()
	case actions.ErrUnknownPushErr:
		status, ok := status.FromError(err)
		if ok && status.Code() == codes.PermissionDenied {
			cli.Println("hint: have you logged into DoltHub using 'dolt login'?")
			cli.Println("hint: check that user.email in 'dolt config --list' has write perms to DoltHub repo")
		}
		if rpcErr, ok := err.(*remotestorage.RpcError); ok {
			return errhand.BuildDError("error: push failed").AddCause(err).AddDetails(rpcErr.FullDetails()).Build()
		} else {
			return errhand.BuildDError("error: push failed").AddCause(err).Build()
		}
	default:
		return errhand.BuildDError("error: push failed").AddCause(err).Build()
	}
	return nil
}

func (ts *TextSpinner) next() string {
	now := time.Now()
	if now.Sub(ts.lastUpdate) > minUpdate {
		ts.seqPos = (ts.seqPos + 1) % len(spinnerSeq)
		ts.lastUpdate = now
	}

	return string([]rune{spinnerSeq[ts.seqPos]})
}

func pullerProgFunc(ctx context.Context, pullerEventCh chan pull.PullerEvent, language progLanguage) {
	var pos int
	var currentTreeLevel int
	var percentBuffered float64
	var tableFilesClosed int
	var filesTransfered int
	var ts TextSpinner

	uploadRate := ""

	for evt := range pullerEventCh {
		if ctx.Err() != nil {
			return
		}
		switch evt.EventType {
		case pull.NewLevelTWEvent:
			if evt.TWEventDetails.TreeLevel != 1 {
				currentTreeLevel = evt.TWEventDetails.TreeLevel
				percentBuffered = 0
			}
		case pull.DestDBHasTWEvent:
			if evt.TWEventDetails.TreeLevel != -1 {
				currentTreeLevel = evt.TWEventDetails.TreeLevel
			}

		case pull.LevelUpdateTWEvent:
			if evt.TWEventDetails.TreeLevel != -1 {
				currentTreeLevel = evt.TWEventDetails.TreeLevel
				toBuffer := evt.TWEventDetails.ChunksInLevel - evt.TWEventDetails.ChunksAlreadyHad

				if toBuffer > 0 {
					percentBuffered = 100 * float64(evt.TWEventDetails.ChunksBuffered) / float64(toBuffer)
				}
			}

		case pull.LevelDoneTWEvent:

		case pull.TableFileClosedEvent:
			tableFilesClosed += 1

		case pull.StartUploadTableFileEvent:

		case pull.UploadTableFileUpdateEvent:
			bps := float64(evt.TFEventDetails.Stats.Read) / evt.TFEventDetails.Stats.Elapsed.Seconds()
			uploadRate = humanize.Bytes(uint64(bps)) + "/s"

		case pull.EndUploadTableFileEvent:
			filesTransfered += 1
		}

		if currentTreeLevel == -1 {
			continue
		}

		var msg string
		msg = fmt.Sprintf("%s Tree Level: %d, Percent Buffered: %.2f%%,", ts.next(), currentTreeLevel, percentBuffered)

		if language == downloadLanguage {
			msg = fmt.Sprintf("%s Files Written: %d", msg, filesTransfered)
		} else {
			if len(uploadRate) > 0 {
				msg = fmt.Sprintf("%s Files Created: %d, Files Uploaded: %d, Current Upload Speed: %s", msg, tableFilesClosed, filesTransfered, uploadRate)
			} else {
				msg = fmt.Sprintf("%s Files Created: %d, Files Uploaded: %d", msg, tableFilesClosed, filesTransfered)
			}
		}

		pos = cli.DeleteAndPrint(pos, msg)
	}
}

func progFunc(ctx context.Context, progChan chan pull.PullProgress) {
	var latest pull.PullProgress
	last := time.Now().UnixNano() - 1
	lenPrinted := 0
	done := false
	for !done {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
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

// progLanguage is the language to use when displaying progress for a pull from a src db to a sink db.
type progLanguage int

const (
	defaultLanguage progLanguage = iota
	downloadLanguage
)

func buildProgStarter(language progLanguage) actions.ProgStarter {
	return func(ctx context.Context) (*sync.WaitGroup, chan pull.PullProgress, chan pull.PullerEvent) {
		pullerEventCh := make(chan pull.PullerEvent, 128)
		progChan := make(chan pull.PullProgress, 128)
		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			progFunc(ctx, progChan)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			pullerProgFunc(ctx, pullerEventCh, language)
		}()

		return wg, progChan, pullerEventCh
	}
}

func stopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, progChan chan pull.PullProgress, pullerEventCh chan pull.PullerEvent) {
	cancel()
	close(progChan)
	close(pullerEventCh)
	wg.Wait()
}

func bytesPerSec(bytes uint64, start time.Time) string {
	bps := float64(bytes) / float64(time.Since(start).Seconds())
	return humanize.Bytes(uint64(bps))
}
