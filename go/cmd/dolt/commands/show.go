// Copyright 2023 Dolthub, Inc.
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

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

type showOpts struct {
	showParents bool
	decoration  string
	specRefs    []string
}

var showDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show information about a specific commit`,
	LongDesc:  `Show information about a specific commit`,
	Synopsis: []string{
		`[{{.LessThan}}revision{{.GreaterThan}}]`,
	},
}

type ShowCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ShowCmd) Name() string {
	return "show"
}

// Description returns a description of the command
func (cmd ShowCmd) Description() string {
	return "Show information about a specific commit."
}

// EventType returns the type of the event to log
func (cmd ShowCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SHOW
}

func (cmd ShowCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(showDocs, ap)
}

func (cmd ShowCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(cli.ParentsFlag, "", "Shows all parents of each commit in the log.")
	ap.SupportsString(cli.DecorateFlag, "", "decorate_fmt", "Shows refs next to commits. Valid options are short, full, no, and auto")
	return ap
}

// Exec executes the command
func (cmd ShowCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, showDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	opts, err := parseShowArgs(ctx, dEnv, apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	err = showCommits(ctx, dEnv, opts)

	if err != nil {
		return handleErrAndExit(err)
	}

	return 0
}

func parseShowArgs(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*showOpts, error) {

	decorateOption := apr.GetValueOrDefault(cli.DecorateFlag, "auto")
	switch decorateOption {
	case "short", "full", "auto", "no":
	default:
		return nil, fmt.Errorf("fatal: invalid --decorate option: %s", decorateOption)
	}

	return &showOpts{
		showParents: apr.Contains(cli.ParentsFlag),
		decoration:  decorateOption,
		specRefs:    apr.Args,
	}, nil
}

func showCommits(ctx context.Context, dEnv *env.DoltEnv, opts *showOpts) error {
	if len(opts.specRefs) == 0 {
		return showCommit(ctx, dEnv, opts, dEnv.RepoStateReader().CWBHeadSpec())
	}

	for _, specRef := range opts.specRefs {
		commitSpec, err := getCommitSpec(specRef)

		if err != nil {
			cli.PrintErrln(color.HiRedString("Fatal error: invalid commit spec %s", specRef))
			return err
		}

		err = showCommit(ctx, dEnv, opts, commitSpec)
		if err != nil {
			return err
		}
	}

	return nil
}

func showCommit(ctx context.Context, dEnv *env.DoltEnv, opts *showOpts, commitSpec *doltdb.CommitSpec) error {

	comm, err := dEnv.DoltDB.Resolve(ctx, commitSpec, dEnv.RepoStateReader().CWBHeadRef())

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot resolve commit spec."))
		return err
	}

	cHashToRefs, err := getHashToRefs(ctx, dEnv, opts.decoration)

	if err != nil {
		return err
	}

	meta, mErr := comm.GetCommitMeta(ctx)
	if mErr != nil {
		cli.PrintErrln("error: failed to get commit metadata")
		return err
	}
	pHashes, pErr := comm.ParentHashes(ctx)
	if pErr != nil {
		cli.PrintErrln("error: failed to get parent hashes")
		return err
	}
	cmHash, cErr := comm.HashOf()
	if cErr != nil {
		cli.PrintErrln("error: failed to get commit hash")
		return err
	}

	headRef := dEnv.RepoStateReader().CWBHeadRef()
	cwbHash, err := dEnv.DoltDB.GetHashForRefStr(ctx, headRef.String())

	if err != nil {
		return err
	}

	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		PrintCommit(pager, 0, opts.showParents, opts.decoration, logNode{
			commitMeta:   meta,
			commitHash:   cmHash,
			parentHashes: pHashes,
			branchNames:  cHashToRefs[cmHash],
			isHead:       cmHash == *cwbHash})
	})

	return nil
}
