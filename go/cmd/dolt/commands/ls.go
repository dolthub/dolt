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

func Ls(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(verboseFlag, "v", "show the hash of the table")
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
	tblNms, err := root.GetTableNames(ctx)
	tblNames := []string{}
	for _, name := range tblNms {
		if name != doltdb.DocTableName {
			tblNames = append(tblNames, name)
		}
	}

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
