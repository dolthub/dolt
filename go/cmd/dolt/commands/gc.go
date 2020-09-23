// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/datas"
)

var gcDocs = cli.CommandDocumentationContent{
	ShortDesc: "List tables",
	LongDesc: `With no arguments lists the tables in the current working set but if a commit is specified it will list the tables in that commit.  If the {{.EmphasisLeft}}--verbose{{.EmphasisRight}} flag is provided a row count and a hash of the table will also be displayed.

If the {{.EmphasisLeft}}--system{{.EmphasisRight}} flag is supplied this will show the dolt system tables which are queryable with SQL.  Some system tables can be queried even if they are not in the working set by specifying appropriate parameters in the SQL queries. To see these tables too you may pass the {{.EmphasisLeft}}--verbose{{.EmphasisRight}} flag.

If the {{.EmphasisLeft}}--all{{.EmphasisRight}} flag is supplied both user and system tables will be printed.
`,

	Synopsis: []string{
		"[--options] [{{.LessThan}}commit{{.GreaterThan}}]",
	},
}

type GarbageCollectionCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd GarbageCollectionCmd) Name() string {
	return "gc"
}

// Description returns a description of the command
func (cmd GarbageCollectionCmd) Description() string {
	return "Cleans up unreferenced data from the database."
}

// Hidden should return true if this command should be hidden from the help text
func (cmd GarbageCollectionCmd) Hidden() bool {
	return true
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd GarbageCollectionCmd) RequiresRepo() bool {
	return true
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd GarbageCollectionCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, gcDocs, ap))
}

func (cmd GarbageCollectionCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Version displays the version of the running dolt client
// Exec executes the command
func (cmd GarbageCollectionCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	_, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, lsDocs, ap))

	var verr errhand.VerboseError

	db, ok := dEnv.DoltDB.ValueReadWriter().(datas.Database)
	if !ok {
		verr = errhand.BuildDError("this database does not support garbage collection").Build()
	}

	err := datas.PruneTableFiles(ctx, db)

	if err != nil {
		verr = errhand.BuildDError("an error occurred during garbage collection").AddCause(err).Build()
	}

	return HandleVErrAndExitCode(verr, usage)
}
