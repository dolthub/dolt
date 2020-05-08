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

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var tblMvDocs = cli.CommandDocumentationContent{
	ShortDesc: "Renames a table",
	LongDesc: `
The dolt table mv command will rename a table. If a table exists with the target name this command will 
fail unless the {{.EmphasisLeft}}--force|-f{{.EmphasisRight}} flag is provided.  In that case the table at the target location will be overwritten 
by the table being renamed.

The result is equivalent of running {{.EmphasisLeft}}dolt table cp <old> <new>{{.EmphasisRight}} followed by {{.EmphasisLeft}}dolt table rm <old>{{.EmphasisRight}}, resulting 
in a new table and a deleted table in the working set. These changes can be staged using {{.EmphasisLeft}}dolt add{{.EmphasisRight}} and committed
using {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.`,

	Synopsis: []string{
		"[-f] {{.LessThan}}oldtable{{.EmphasisRight}} {{.LessThan}}newtable{{.EmphasisRight}}",
	},
}

type MvCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MvCmd) Name() string {
	return "mv"
}

// Description returns a description of the command
func (cmd MvCmd) Description() string {
	return "Moves a table"
}

// BuildCommandDocumentation creates a markdown file containing the helptext for the command at the given path
func (cmd MvCmd) GetCommandDocumentation(commandStr string) cli.CommandDocumentation {
	ap := cmd.createArgParser()
	return cli.BuildCommandDocumentation(commandStr, tblMvDocs, ap)
}

func (cmd MvCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"oldtable", "The table being moved."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"newtable", "The new name of the table"})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the Force flag will allow the target to be overwritten.")
	return ap
}

// EventType returns the type of the event to log
func (cmd MvCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_MV
}

// Exec executes the command
func (cmd MvCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.BuildCommandDocumentation(commandStr, tblMvDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	force := apr.Contains(forceParam)

	working, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	oldName := apr.Arg(0)
	newName := apr.Arg(1)

	if doltdb.HasDoltPrefix(oldName) {
		return commands.HandleVErrAndExitCode(
			errhand.BuildDError("error renaming table %s", oldName).AddCause(doltdb.ErrSystemTableCannotBeModified).Build(), usage)
	}

	if verr = ValidateTableNameForCreate(newName); verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	_, ok, err := working.GetTable(ctx, oldName)

	if err != nil {
		verr = errhand.BuildDError("error: failed to read tables from working set").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	if !ok {
		verr = errhand.BuildDError("Table '%s' not found.", oldName).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	has, err := working.HasTable(ctx, newName)

	if err != nil {
		verr = errhand.BuildDError("error: failed to read tables from working set").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	} else if !force && has {
		verr = errhand.BuildDError("Data already exists in '%s'.  Use -f to overwrite.", newName).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	working, err = working.RenameTable(ctx, oldName, newName)
	if err != nil {
		verr = errhand.BuildDError("Unable to remove '%s'", oldName).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	return commands.HandleVErrAndExitCode(commands.UpdateWorkingWithVErr(dEnv, working), usage)
}
