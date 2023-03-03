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

package stashcmds

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
)

var ErrStashNotSupportedForOldFormat = errors.New("stash is not supported for old storage format")

var StashCommands = cli.NewSubCommandHandlerWithUnspecified("stash", "Stash the changes in a dirty working directory away.", false, StashCmd{}, []cli.Command{
	StashClearCmd{},
	StashDropCmd{},
	StashListCmd{},
	StashPopCmd{},
})

var stashDocs = cli.CommandDocumentationContent{
	ShortDesc: "Stash the changes in a dirty working directory away.",
	LongDesc: `Use dolt stash when you want to record the current state of the working directory and the index, but want to go back to a clean working directory. 

The command saves your local modifications away and reverts the working directory to match the HEAD commit.
`,
	Synopsis: []string{
		"list",
		"pop {{.LessThan}}stash{{.GreaterThan}}",
		"clear",
		"drop {{.LessThan}}stash{{.GreaterThan}}",
	},
}

type StashCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashCmd) Name() string {
	return "stash"
}

// Description returns a description of the command
func (cmd StashCmd) Description() string {
	return "Stash the changes in a dirty working directory away."
}

func (cmd StashCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashDocs, ap)
}

func (cmd StashCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// EventType returns the type of the event to log
func (cmd StashCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH
}

// Exec executes the command
func (cmd StashCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	if !dEnv.DoltDB.Format().UsesFlatbuffers() {
		cli.PrintErrln(ErrStashNotSupportedForOldFormat.Error())
		return 1
	}
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if dEnv.IsLocked() {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	if apr.NArg() > 0 {
		usage()
		return 1
	}

	err := stashChanges(ctx, dEnv)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return 0
}

func stashChanges(ctx context.Context, dEnv *env.DoltEnv) error {
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return err
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}
	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return err
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return err
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return err
	}
	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return err
	}

	if headHash.Equal(workingHash) && headHash.Equal(stagedHash) {
		cli.Println("No local changes to save")
		return nil
	}

	// TODO: handle cases with staged changes?
	if !headHash.Equal(stagedHash) {
		return fmt.Errorf("Stashing staged set of changes support is coming soon")
	}

	curHeadRef := dEnv.RepoStateReader().CWBHeadRef()
	curBranchName := curHeadRef.String()
	commitSpec, err := doltdb.NewCommitSpec(curBranchName)
	if err != nil {
		return err
	}
	commit, err := dEnv.DoltDB.Resolve(ctx, commitSpec, curHeadRef)
	if err != nil {
		return err
	}
	commitHash, err := commit.HashOf()
	if err != nil {
		return err
	}
	commitMeta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return err
	}

	err = dEnv.DoltDB.AddStash(ctx, commit, workingRoot, datas.NewStashMeta(curBranchName, commitMeta.Description))
	if err != nil {
		return err
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}
	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(headRoot))
	if err != nil {
		return err
	}

	cli.Println(fmt.Sprintf("Saved working directory and index state WIP on %s: %s %s", curBranchName, commitHash.String(), commitMeta.Description))
	return nil
}
