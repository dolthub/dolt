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
	"sort"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var lsShortDesc = "List tables"
var lsLongDesc = "Lists the tables within a commit.  By default will list the tables in the current working set" +
	"but if a commit is specified it will list the tables in that commit."
var lsSynopsis = []string{
	"[<commit>]",
}

type LsCmd struct{}

func (cmd LsCmd) Name() string {
	return "ls"
}

func (cmd LsCmd) Description() string {
	return "List tables in the working set."
}

func (cmd LsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return cli.CreateMarkdown(fs, path, commandStr, lsShortDesc, lsLongDesc, lsSynopsis, ap)
}

func (cmd LsCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(verboseFlag, "v", "show the hash of the table")
	return ap
}

func (cmd LsCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LS
}

func (cmd LsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, lsShortDesc, lsLongDesc, lsSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() > 1 {
		usage()
		return 1
	}

	var root *doltdb.RootValue
	var verr errhand.VerboseError
	var label string
	if apr.NArg() == 0 {
		label = "working set"
		root, verr = GetWorkingWithVErr(dEnv)
	} else {
		label, root, verr = getRootForCommitSpecStr(ctx, apr.Arg(0), dEnv)
	}

	if verr == nil {
		verr = printTables(ctx, root, label, apr.Contains(verboseFlag))
		return 0
	}

	cli.PrintErrln(verr.Verbose())
	return 1
}

func printTables(ctx context.Context, root *doltdb.RootValue, label string, verbose bool) errhand.VerboseError {
	tblNames, err := root.GetTableNames(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to get tables").AddCause(err).Build()
	}

	sort.Strings(tblNames)

	if len(tblNames) == 0 {
		cli.Println("No tables in", label)
		return nil
	}

	cli.Printf("Tables in %s:\n", label)
	for _, tbl := range tblNames {
		if verbose {
			h, _, err := root.GetTableHash(ctx, tbl)

			if err != nil {
				return errhand.BuildDError("error: failed to get table hash").AddCause(err).Build()
			}

			tblVal, _, err := root.GetTable(ctx, tbl)

			if err != nil {
				return errhand.BuildDError("error: failed to get table").AddCause(err).Build()
			}

			rows, err := tblVal.GetRowData(ctx)

			if err != nil {
				return errhand.BuildDError("error: failed to get row data").AddCause(err).Build()
			}

			cli.Printf("\t%-32s %s    %d rows\n", tbl, h.String(), rows.Len())
		} else {
			cli.Println("\t", tbl)
		}
	}

	return nil
}
