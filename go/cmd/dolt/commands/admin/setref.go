// Copyright 2022 Dolthub, Inc.
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

package admin

import (
	"context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
)

type SetRefCmd struct {
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd SetRefCmd) Name() string {
	return "set-ref"
}

// Description returns a description of the command
func (cmd SetRefCmd) Description() string {
	return "Sets a ref in the root refs map directly to a desired hash"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd SetRefCmd) RequiresRepo() bool {
	return true
}

func (cmd SetRefCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd SetRefCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsOptionalString("branch", "", "branch name", "the branch ref to set")
	ap.SupportsOptionalString("remote-name", "", "remote name", "the remote name, e.g. origin, of the remote ref to set")
	ap.SupportsOptionalString("remote-branch", "", "remote branch name", "the remote branch name of the remote ref set")
	ap.SupportsString("to", "", "commit-hash", "the commit hash to set the ref to")
	return ap
}

func (cmd SetRefCmd) Hidden() bool {
	return true
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd SetRefCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))

	apr := cli.ParseArgsOrDie(ap, args, usage)

	var r ref.DoltRef

	if apr.Contains("branch") {
		if apr.Contains("remote-name") || apr.Contains("remote-branch") {
			verr := errhand.BuildDError("--branch and --branch-name / --remote-branch are mutually exclusive").SetPrintUsage().Build()
			commands.HandleVErrAndExitCode(verr, usage)
		}
		r = ref.NewBranchRef(apr.MustGetValue("branch"))
	} else if apr.Contains("remote-name") || apr.Contains("remote-branch") {
		if !(apr.Contains("remote-branch") && apr.Contains("remote-name")) {
			verr := errhand.BuildDError("--branch-name and --remote-branch must both be supplied").SetPrintUsage().Build()
			commands.HandleVErrAndExitCode(verr, usage)
		}
		r = ref.NewRemoteRef(apr.MustGetValue("remote-name"), apr.MustGetValue("remote-branch"))
	}

	h := hash.Parse(apr.MustGetValue("to"))

	err := dEnv.DoltDB(ctx).SetHead(ctx, r, h)
	if err != nil {
		verr := errhand.BuildDError("error setting %s to %s", r.String(), h.String()).AddCause(err).Build()
		commands.HandleVErrAndExitCode(verr, usage)
	}

	return 0
}
