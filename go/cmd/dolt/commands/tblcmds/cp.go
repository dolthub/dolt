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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var tblCpShortDesc = "Makes a copy of a table"
var tblCpLongDesc = `The dolt table cp command makes a copy of a table at a given commit.  If a commit is not specified the copy is made of
the current working table.

If a table exists at the target location this command will fail unless the <b>--force|-f</b> flag is provided.  In this
case the table at the target location will be overwritten with the copied table.

All changes will be applied to the working tables and will need to be staged using <b>dolt add</b> and committed
using <b>dolt commit</b>.`

var tblCpSynopsis = []string{
	"[-f] [<commit>] <oldtable> <newtable>",
}

func Cp(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["commit"] = "The state at which point the table whill be copied."
	ap.ArgListHelp["oldtable"] = "The table being copied."
	ap.ArgListHelp["newtable"] = "The destination where the table is being copied to."
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the Force flag will allow the target to be overwritten.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblCpShortDesc, tblCpLongDesc, tblCpSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() < 2 || apr.NArg() > 3 {
		usage()
		return 1
	}

	force := apr.Contains(forceParam)
	working, verr := commands.GetWorkingWithVErr(dEnv)
	root := working

	if verr == nil {
		var old, new string
		if apr.NArg() == 3 {
			var cm *doltdb.Commit
			cm, verr = commands.ResolveCommitWithVErr(dEnv, apr.Arg(0), dEnv.RepoState.Head.Ref.String())
			if verr == nil {
				var err error
				root, err = cm.GetRootValue()

				if err != nil {
					verr = errhand.BuildDError("error: failed to get root value").AddCause(err).Build()
				}
			}

			old, new = apr.Arg(1), apr.Arg(2)
		} else {
			old, new = apr.Arg(0), apr.Arg(1)
		}

		if verr == nil {
			tbl, ok, err := root.GetTable(ctx, old)

			if err != nil {
				verr = errhand.BuildDError("error: failed to get table").AddCause(err).Build()
			}

			if ok && verr == nil {
				has, err := working.HasTable(ctx, new)

				if err != nil {
					verr = errhand.BuildDError("error: failed to get tables").AddCause(err).Build()
				} else if !force && has {
					verr = errhand.BuildDError("Data already exists in '%s'.  Use -f to overwrite.", new).Build()
				} else {
					working, err = working.PutTable(ctx, new, tbl)

					if err != nil {
						verr = errhand.BuildDError("error; failed to write tables back to database").Build()
					} else {
						verr = commands.UpdateWorkingWithVErr(dEnv, working)
					}
				}
			} else if verr == nil {
				verr = errhand.BuildDError("Table '%s' not found in root", old).Build()
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
