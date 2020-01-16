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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var tblMvShortDesc = "Renames a table"
var tblMvLongDesc = `
The dolt table mv command will rename a table. If a table exists with the target name this command will 
fail unless the <b>--force|-f</b> flag is provided.  In that case the table at the target location will be overwritten 
by the table being renamed.

The result is equivalent of running <b>dolt table cp <old> <new></b> followed by <b>dolt table rm <old></b>, resulting 
in a new table and a deleted table in the working set. These changes can be staged using <b>dolt add</b> and committed
using <b>dolt commit</b>.`

var tblMvSynopsis = []string{
	"[-f] <oldtable> <newtable>",
}

type MvCmd struct{}

func (cmd MvCmd) Name() string {
	return "mv"
}

func (cmd MvCmd) Description() string {
	return "Moves a table"
}

func (cmd MvCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, tblMvShortDesc, tblMvLongDesc, tblMvSynopsis, ap)
}

func (cmd MvCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"oldtable", "The table being moved."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"newtable", "The new name of the table"})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the Force flag will allow the target to be overwritten.")
	return ap
}

func (cmd MvCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_MV
}

func (cmd MvCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblMvShortDesc, tblMvLongDesc, tblMvSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	force := apr.Contains(forceParam)
	working, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		old := apr.Arg(0)
		new := apr.Arg(1)
		if verr == nil {
			tbl, ok, err := working.GetTable(ctx, old)

			if err != nil {
				verr = errhand.BuildDError("").Build()
				return commands.HandleVErrAndExitCode(verr, usage)
			}

			if ok {
				has, err := working.HasTable(ctx, new)

				if err != nil {
					verr = errhand.BuildDError("error: failed to read tables from working set").AddCause(err).Build()
				} else if !force && has {
					verr = errhand.BuildDError("Data already exists in '%s'.  Use -f to overwrite.", new).Build()
				} else {
					working, err = working.PutTable(ctx, new, tbl)

					if err != nil {
						verr = errhand.BuildDError("error: failed to write table back to database").AddCause(err).Build()
					} else {
						working, err := working.RemoveTables(ctx, old)

						if err != nil {
							verr = errhand.BuildDError("Unable to remove '%s'", old).Build()
						} else {
							verr = commands.UpdateWorkingWithVErr(dEnv, working)
						}
					}
				}
			} else {
				verr = errhand.BuildDError("Table '%s' not found.", old).Build()
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
