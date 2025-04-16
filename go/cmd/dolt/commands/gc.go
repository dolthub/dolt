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

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

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

var gcDocs = cli.CommandDocumentationContent{
	ShortDesc: "Cleans up unreferenced data from the repository.",
	LongDesc: `Searches the repository for data that is no longer referenced and no longer needed.

Dolt GC is generational. When a GC is run, everything reachable from any commit on any branch
is put into the old generation. Data which is only reachable from uncommited branch HEADs is kept in
the new generation. By default, Dolt GC will only visit data in the new generation, and so will never
collect data from deleted branches which has previously made its way to the old generation from being
copied during a prior garbage collection.

If the {{.EmphasisLeft}}--shallow{{.EmphasisRight}} flag is supplied, a faster but less thorough garbage collection will be performed.

If the {{.EmphasisLeft}}--full{{.EmphasisRight}} flag is supplied, a more thorough garbage collection, fully collecting the old gen and new gen, will be performed.`,
	Synopsis: []string{
		"[--shallow|--full]",
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

func (cmd GarbageCollectionCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(gcDocs, ap)
}

func (cmd GarbageCollectionCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateGCArgParser()
}

// EventType returns the type of the event to log
func (cmd GarbageCollectionCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_GARBAGE_COLLECTION
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd GarbageCollectionCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, gcDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.Contains(cli.ShallowFlag) && apr.Contains(cli.FullFlag) {
		return HandleVErrAndExitCode(errhand.BuildDError("Invalid Argument: --shallow is not compatible with --full").SetPrintUsage().Build(), usage)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query, err := cmd.constructDoltGCQuery(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	_, _, _, err = queryist.Query(sqlCtx, query)
	if err != nil && err != chunks.ErrNothingToCollect {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return HandleVErrAndExitCode(nil, usage)
}

// constructDoltGCQuery generates the sql query necessary to call DOLT_GC()
func (cmd GarbageCollectionCmd) constructDoltGCQuery(apr *argparser.ArgParseResults) (string, error) {
	var params []interface{}

	extraFlag := ""
	if apr.Contains(cli.ShallowFlag) {
		extraFlag = "--shallow"
	} else if apr.Contains(cli.FullFlag) {
		extraFlag = "--full"
	}

	archiveLevel := chunks.NoArchive
	if apr.Contains(cli.ArchiveLevelParam) {
		lvl, ok := apr.GetInt(cli.ArchiveLevelParam)
		if !ok {
			return "", errhand.BuildDError("Invalid Argument: --archive-level must be an integer").SetPrintUsage().Build()
		}
		// validation is done in the procedure.
		archiveLevel = chunks.GCArchiveLevel(lvl)
	}

	params = append(params, archiveLevel)

	query := "CALL DOLT_GC('--archive-level', ?"
	if extraFlag != "" {
		query += ", ?)"
		params = append(params, extraFlag)
	} else {
		query += ")"
	}

	return dbr.InterpolateForDialect(query, params, dialect.MySQL)
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
	if tmp.DBLoadError != nil {
		return nil, tmp.DBLoadError
	}

	return tmp, nil
}
