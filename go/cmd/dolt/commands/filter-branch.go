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
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var filterBranchDocs = cli.CommandDocumentationContent{
	ShortDesc: "",
	LongDesc: ``,

	Synopsis: []string{
		"",
	},
}

type FilterBranchCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd FilterBranchCmd) Name() string {
	return "filter-branch"
}

// Description returns a description of the command
func (cmd FilterBranchCmd) Description() string {
	return "Edits the commit history using the provided query."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd FilterBranchCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, filterBranchDocs, ap))
}

func (cmd FilterBranchCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(allFlag, "a", "filter all branches")
	return ap
}

// EventType returns the type of the event to log
// todo: make event
//func (cmd FilterBranchCmd) EventType() eventsapi.ClientEventType {
//	return eventsapi.ClientEventType_LS
//}

// Exec executes the command
func (cmd FilterBranchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, filterBranchDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	var verr errhand.VerboseError

	if apr.Contains(allFlag) {
		cli.Println(allFlag)
	}

	return HandleVErrAndExitCode(verr, usage)
}
