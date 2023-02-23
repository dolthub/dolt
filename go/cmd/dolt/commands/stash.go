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
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var stashDocs = cli.CommandDocumentationContent{
	ShortDesc: `Stash the changes in a dirty working directory away.`,
	LongDesc: `

`,
	Synopsis: []string{
		`{{.LessThan}}stash{{.GreaterThan}}`,
	},
}

type StashCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command.
func (cmd StashCmd) Name() string {
	return "stash"
}

// Description returns a description of the command.
func (cmd StashCmd) Description() string {
	return "Stash the changes in a dirty working directory away."
}

func (cmd StashCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateStashArgParser()
	return cli.NewCommandDocumentation(stashDocs, ap)
}

func (cmd StashCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateStashArgParser()
}

// EventType returns the type of the event to log.
func (cmd StashCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH
}

// Exec executes the command.
func (cmd StashCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateStashArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if dEnv.IsLocked() {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	// TODO: these needs to be subcommands NOT flag options...
	var verr errhand.VerboseError
	switch {
	case apr.Contains(cli.ListFlag):
		verr = listStashes(ctx, dEnv)
	case apr.Contains(cli.PopFlag):
		verr = popStash(ctx, dEnv)
	case apr.Contains(cli.ClearFlag):
		verr = clearStashes(ctx, dEnv)
	default:
		verr = stashChanges(ctx, dEnv)
	}
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}
	return 0
}

func listStashes(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	return nil
}

func popStash(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	return nil
}

func clearStashes(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	return nil
}

func stashChanges(ctx context.Context, dEnv *env.DoltEnv) errhand.VerboseError {
	// check for clean working state
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	if headHash.Equal(workingHash) && headHash.Equal(stagedHash) {
		return errhand.BuildDError("No local changes to save").Build()
	}

	curBranch := dEnv.RepoStateReader().CWBHeadRef().String()
	cms, err := doltdb.NewCommitSpec(curBranch)
	commit, err := dEnv.DoltDB.Resolve(ctx, cms, dEnv.RepoStateReader().CWBHeadRef())
	cmm, err := commit.GetCommitMeta(ctx)

	if cmm == nil {
	}

	return nil
}
