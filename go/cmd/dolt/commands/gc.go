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
	"io"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/nbs"
)

const (
	gcShallowFlag = "shallow"
)

var gcDocs = cli.CommandDocumentationContent{
	ShortDesc: "Cleans up unreferenced data from the repository.",
	LongDesc: `Searches the repository for data that is no longer referenced and no longer needed.

If the {{.EmphasisLeft}}--shallow{{.EmphasisRight}} flag is supplied, a faster but less thorough garbage collection will be performed.`,
	Synopsis: []string{
		"[--shallow]",
	},
}

type GarbageCollectionCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd GarbageCollectionCmd) Name() string {
	return "gc"
}

// Description returns a description of the command
func (cmd GarbageCollectionCmd) Description() string {
	return gcDocs.ShortDesc
}

// Hidden should return true if this command should be hidden from the help text
func (cmd GarbageCollectionCmd) Hidden() bool {
	return false
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd GarbageCollectionCmd) RequiresRepo() bool {
	return true
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd GarbageCollectionCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, gcDocs, ap))
}

func (cmd GarbageCollectionCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(gcShallowFlag, "s", "perform a fast, but incomplete garbage collection pass")
	return ap
}

// EventType returns the type of the event to log
func (cmd GarbageCollectionCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_GARBAGE_COLLECTION
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd GarbageCollectionCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	var verr errhand.VerboseError

	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, gcDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	var err error
	if apr.Contains(gcShallowFlag) {
		err = dEnv.DoltDB.ShallowGC(ctx)
		if err != nil {
			if err == chunks.ErrUnsupportedOperation {
				verr = errhand.BuildDError("this database does not support shallow garbage collection").Build()
				return HandleVErrAndExitCode(verr, usage)
			}
			verr = errhand.BuildDError("an error occurred during garbage collection").AddCause(err).Build()
		}
	} else {
		// full gc
		dEnv, err = MaybeMigrateEnv(ctx, dEnv)

		if err != nil {
			verr = errhand.BuildDError("could not load manifest for gc").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		}

		keepers, err := env.GetGCKeepers(ctx, dEnv)
		if err != nil {
			verr = errhand.BuildDError("an error occurred while saving working set").AddCause(err).Build()
			return HandleVErrAndExitCode(verr, usage)
		}

		err = dEnv.DoltDB.GC(ctx, keepers...)
		if err != nil {
			if errors.Is(err, chunks.ErrNothingToCollect) {
				cli.PrintErrln(color.YellowString("Nothing to collect."))
			} else {
				verr = errhand.BuildDError("an error occurred during garbage collection").AddCause(err).Build()
			}
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func MaybeMigrateEnv(ctx context.Context, dEnv *env.DoltEnv) (*env.DoltEnv, error) {
	migrated, err := nbs.MaybeMigrateFileManifest(ctx, dbfactory.DoltDataDir)
	if err != nil {
		return nil, err
	}
	if !migrated {
		return dEnv, nil
	}

	// reload env with new manifest
	tmp := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, dEnv.Version)

	if tmp.CfgLoadErr != nil {
		return nil, tmp.CfgLoadErr
	}
	if tmp.RSLoadErr != nil {
		return nil, tmp.RSLoadErr
	}
	if tmp.DocsLoadErr != nil {
		return nil, tmp.DocsLoadErr
	}
	if tmp.DBLoadError != nil {
		return nil, tmp.DBLoadError
	}

	return tmp, nil
}
