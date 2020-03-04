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

package tblcmds

import (
	"context"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var tblRmShortDesc = "Removes table(s) from the working set of tables."
var tblRmLongDesc = "{{.EmphasisLeft}}dolt table rm{{.EmphasisRight}} removes table(s) from the working set.  These changes can be staged using {{.EmphasisLeft}}dolt add{{.EmphasisRight}} and committed using {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}"
var tblRmSynopsis = []string{
	"{{.LessThan}}table{{.GreaterThan}}...",
}

var tblRmDocumentation = cli.CommandDocumentation{
	ShortDesc: tblRmShortDesc,
	LongDesc: tblRmLongDesc,
	Synopsis: tblRmSynopsis,
}

type RmCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RmCmd) Name() string {
	return "rm"
}

// Description returns a description of the command
func (cmd RmCmd) Description() string {
	return "Deletes a table"
}

// EventType returns the type of the event to log
func (cmd RmCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_RM
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd RmCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, commandStr, tblRmDocumentation, ap)
}

func (cmd RmCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table to remove"})
	return ap
}

// Exec executes the command
func (cmd RmCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblRmDocumentation, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 1
	}

	for _, tableName := range apr.Args() {
		if doltdb.IsSystemTable(tableName) {
			return commands.HandleVErrAndExitCode(
				errhand.BuildDError("error removing table %s", tableName).AddCause(doltdb.ErrSystemTableCannotBeModified).Build(), usage)
		}
	}

	working, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr := commands.ValidateTablesWithVErr(apr.Args(), working)

		if verr == nil {
			verr = removeTables(ctx, dEnv, apr.Args(), working)
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func removeTables(ctx context.Context, dEnv *env.DoltEnv, tables []string, working *doltdb.RootValue) errhand.VerboseError {
	working, err := working.RemoveTables(ctx, tables...)

	if err != nil {
		return errhand.BuildDError("Unable to remove table(s)").AddCause(err).Build()
	}

	return commands.UpdateWorkingWithVErr(dEnv, working)
}
