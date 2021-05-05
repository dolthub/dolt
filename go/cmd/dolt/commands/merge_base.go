// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var mergeBaseDocs = cli.CommandDocumentationContent{
	ShortDesc: `Find the common ancestor of two commits.`,
	LongDesc:  `Find the common ancestor of two commits.`,
	Synopsis:  []string{
		`{{.LessThan}}commit spec{{.GreaterThan}} {{.LessThan}}commit spec{{.GreaterThan}}`,
	},
}

type MergeBaseCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MergeBaseCmd) Name() string {
	return "merge-base"
}

// Description returns a description of the command
func (cmd MergeBaseCmd) Description() string {
	return mergeBaseDocs.ShortDesc
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd MergeBaseCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, mergeBaseDocs, ap))
}

func (cmd MergeBaseCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	//ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"start-point", "A commit that a new branch should point at."})
	return ap
}

// EventType returns the type of the event to log
func (cmd MergeBaseCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TYPE_UNSPECIFIED
}

// Exec executes the command
func (cmd MergeBaseCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, mergeBaseDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	var verr errhand.VerboseError
	if apr.NArg() != 2 {
		verr = errhand.BuildDError("%s takes exactly 2 args", cmd.Name()).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	left, verr := ResolveCommitWithVErr(dEnv, apr.Arg(0))
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	right, verr := ResolveCommitWithVErr(dEnv, apr.Arg(1))
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	mergeBase, err := merge.MergeBase(ctx, left, right)
	if err != nil {
		verr = errhand.BuildDError("could not find merge-base for args %s", apr.Args()).AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	cli.Println(mergeBase.String())
	return 0
}
