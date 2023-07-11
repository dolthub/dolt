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
	"errors"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dustin/go-humanize"
)

type remoteInfo struct {
	Name       string
	Url        string
	FetchSpecs []string
	Params     map[string]string
}

type remoteBranchInfo struct {
	Name string
	Hash string
}

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

func (cmd PushCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(pushDocs, ap)
}

func (cmd PushCmd) ArgParser() *argparser.ArgParser {
	return cli.CreatePushArgParser()
}

// EventType returns the type of the event to log
func (cmd PushCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PUSH
}

// Exec executes the command
func (cmd PushCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, pushDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleStatusVErr(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}
	err = push(queryist, sqlCtx, apr)
	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}

func push(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults) error {
	force := apr.Contains(cli.ForceFlag)
	setUpstream := apr.Contains(cli.SetUpstreamFlag)
	if setUpstream && apr.NArg() < 2 {
		return errors.New("error: --set-upstream requires <remote> and <refspec> params.")
	}

	branchName, err := getActiveBranchName(sqlCtx, queryist)

	remoteName := "origin"
	remotes, err := getRemotes(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("failed to get remotes: %w", err)
	}
	remoteBranches, err := getRemoteBranches(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("failed to get remote branches: %w", err)
	}

	args := apr.Args
	if len(args) == 1 {
		if _, ok := remotes[args[0]]; ok {
			remoteName = args[0]
			args = []string{}
		}
	}
	targetRemote, remoteOK := remotes[remoteName]
	var refSpec string
	if remoteOK && len(args) == 1 {
		refSpec = args[0]
	} else if len(args) == 2 {
		remoteName = args[0]
		refSpec = args[1]
		if len(refSpec) == 0 {
			return fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, refSpec)
		}
	}

	params := []interface{}{}
	sb := strings.Builder{}
	sb.WriteString("call dolt_push(")
	if force {
		sb.WriteString("'--force', ")
	}
	if setUpstream {
		sb.WriteString("'--set-upstream', ")
	}
	sb.WriteString("?")
	params = append(params, remoteName)
	if len(refSpec) > 0 {
		sb.WriteString(", ?")
		params = append(params, refSpec)
	}
	sb.WriteString(");")
	query := sb.String()

	_, err = InterpolateAndRunQuery(queryist, sqlCtx, query, params...)
	if err != nil {
		text := err.Error()
		if strings.Contains(text, "the current branch has no upstream branch") {
			return fmt.Errorf("fatal: The current branch " + branchName + " has no upstream branch.\n" +
				"To push the current branch and set the remote as upstream, use\n" +
				"\tdolt push --set-upstream " + remoteName + " " + branchName + "\n" +
				"To have this happen automatically for branches without a tracking\n" +
				"upstream, see 'push.autoSetupRemote' in 'dolt config --help'.")
		}
		if strings.Contains(text, "upstream branch already set for") {
			// success
			return nil
		}
		if strings.Contains(text, doltdb.ErrIsAhead.Error()) ||
			strings.Contains(text, actions.ErrCantFF.Error()) {
			srcRef, destRef, err := getTrackingRefs(branchName, targetRemote)
			if err != nil {
				return fmt.Errorf("failed to get tracking ref info: %w", err)
			}

			cli.Printf("To %s\n", targetRemote.Url)
			cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", srcRef, destRef)
			cli.Printf("error: failed to push some refs to '%s'\n", targetRemote.Url)
			cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
			cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
			cli.Println("hint: 'dolt pull ...') before pushing again.")
		}
		return err
	}

	postPushRemoteBranches, err := getRemoteBranches(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("failed to get post-push remote branches: %w", err)
	}

	changesMade := getChangesMade(remoteBranches, postPushRemoteBranches)
	if !changesMade {
		cli.Println("Everything up-to-date")
	}

	return nil
}

func getChangesMade(remoteBranches map[string]remoteBranchInfo, postPushRemoteBranches map[string]remoteBranchInfo) bool {
	changesMade := false
	if len(remoteBranches) != len(postPushRemoteBranches) {
		changesMade = true
	} else {
		for name, remoteBranch := range remoteBranches {
			updatedRemoteBranch, ok := postPushRemoteBranches[name]
			if !ok {
				changesMade = true
				break
			}
			if remoteBranch.Hash != updatedRemoteBranch.Hash {
				changesMade = true
				break
			}
		}
	}
	return changesMade
}

func getRemotes(queryist cli.Queryist, sqlCtx *sql.Context) (map[string]remoteInfo, error) {
	rows, err := GetRowsForSql(queryist, sqlCtx, "select * from dolt_remotes")
	if err != nil {
		return nil, fmt.Errorf("failed to read dolt remotes: %w", err)
	}

	remotes := map[string]remoteInfo{}
	for _, row := range rows {
		name := row[0].(string)
		url := row[1].(string)

		fetchSpecsJson, err := getJsonDocumentCol(sqlCtx, row[2])
		if err != nil {
			return nil, fmt.Errorf("failed to read fetch specs for remote %s: %w", name, err)
		}
		fetchSpecsArray, ok := fetchSpecsJson.Val.([]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to read fetch specs for remote %s", name)
		}
		fetchSpecs := []string{}
		for _, spec := range fetchSpecsArray {
			text, ok := spec.(string)
			if !ok {
				return nil, fmt.Errorf("failed to read fetch specs for remote %s: %w", name, err)
			}
			fetchSpecs = append(fetchSpecs, text)
		}

		paramsJson, err := getJsonDocumentCol(sqlCtx, row[3])
		if err != nil {
			return nil, fmt.Errorf("failed to read params for remote %s: %w", name, err)
		}
		var paramsMap map[string]interface{}
		if paramsJson.Val == nil {
			paramsMap = map[string]interface{}{}
		} else {
			paramsMap, ok = paramsJson.Val.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("failed to read params for remote %s: %v", name, paramsJson.Val)
			}
		}
		params := map[string]string{}
		for k, v := range paramsMap {
			text, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("failed to read params for remote %s: %w", name, err)
			}
			params[k] = text
		}

		remote := remoteInfo{
			Name:       name,
			Url:        url,
			FetchSpecs: fetchSpecs,
			Params:     params,
		}
		remotes[name] = remote
	}
	return remotes, nil
}

func getRemoteBranches(queryist cli.Queryist, sqlCtx *sql.Context) (map[string]remoteBranchInfo, error) {
	rows, err := GetRowsForSql(queryist, sqlCtx, "select name, hash from dolt_remote_branches")
	if err != nil {
		return nil, fmt.Errorf("failed to read dolt remote branchess: %w", err)
	}

	rbs := map[string]remoteBranchInfo{}
	for _, row := range rows {
		name := row[0].(string)
		hash := row[1].(string)
		branch := remoteBranchInfo{
			Name: name,
			Hash: hash,
		}
		rbs[name] = branch
	}
	return rbs, nil
}

func getTrackingRefs(branchName string, info remoteInfo) (fromRef, toRef string, err error) {
	branchRef := ref.NewBranchRef(branchName)

	for _, spec := range info.FetchSpecs {
		fs, err := ref.ParseRefSpecForRemote(info.Name, spec)
		if err != nil {
			return "", "", err
		}
		destRef := fs.DestRef(branchRef)
		if destRef != nil {
			srcRef := fs.SrcRef(branchRef)
			return srcRef.String(), destRef.String(), nil
		}
	}
	return "", "", nil
}

// Below functions are not used in the current implementation, but are used by other codepaths

func pullerProgFunc(ctx context.Context, statsCh chan pull.Stats, language progLanguage) {
	p := cli.NewEphemeralPrinter()

	for {
		select {
		case <-ctx.Done():
			return
		case stats, ok := <-statsCh:
			if !ok {
				return
			}
			if language == downloadLanguage {
				p.Printf("Downloaded %s chunks, %s @ %s/s.",
					humanize.Comma(int64(stats.FetchedSourceChunks)),
					humanize.Bytes(stats.FetchedSourceBytes),
					humanize.SIWithDigits(stats.FetchedSourceBytesPerSec, 2, "B"),
				)
			} else {
				p.Printf("Uploaded %s of %s @ %s/s.",
					humanize.Bytes(stats.FinishedSendBytes),
					humanize.Bytes(stats.BufferedSendBytes),
					humanize.SIWithDigits(stats.SendBytesPerSec, 2, "B"),
				)
			}
			p.Display()
		}
	}
}

// progLanguage is the language to use when displaying progress for a pull from a src db to a sink db.
type progLanguage int

const (
	defaultLanguage progLanguage = iota
	downloadLanguage
)

func buildProgStarter(language progLanguage) actions.ProgStarter {
	return func(ctx context.Context) (*sync.WaitGroup, chan pull.Stats) {
		statsCh := make(chan pull.Stats, 128)
		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			pullerProgFunc(ctx, statsCh, language)
		}()

		return wg, statsCh
	}
}

func stopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, statsCh chan pull.Stats) {
	cancel()
	close(statsCh)
	wg.Wait()
}
