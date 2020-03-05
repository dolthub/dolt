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
	"io"
	"sort"
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/funcitr"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
)

const (
	systemFlag = "system"
)

var lsShortDesc = "List tables"
var lsLongDesc = `With no arguments lists the tables in the current working set but if a commit is specified it will list the tables in that commit.  If the {{.EmphasisLeft}}--verbose{{.EmphasisRight}} flag is provided a row count and a hash of the table will also be displayed.

If the {{.EmphasisLeft}}--system{{.EmphasisRight}} flag is supplied this will show the dolt system tables which are queryable with SQL.  Some system tables can be queried even if they are not in the working set by specifying appropriate parameters in the SQL queries. To see these tables too you may pass the {{.EmphasisLeft}}--verbose{{.EmphasisRight}} flag.

If the {{.EmphasisLeft}}--all{{.EmphasisRight}} flag is supplied both user and system tables will be printed.
`

var lsSynopsis = []string{
	"[--options] [{{.LessThan}}commit{{.GreaterThan}}]",
}

var lsDocumentation = cli.CommandDocumentation{
	ShortDesc: lsShortDesc,
	LongDesc:  lsLongDesc,
	Synopsis:  lsSynopsis,
}

type LsCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LsCmd) Name() string {
	return "ls"
}

// Description returns a description of the command
func (cmd LsCmd) Description() string {
	return "List tables in the working set."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd LsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, commandStr, lsDocumentation, ap)
}

func (cmd LsCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(verboseFlag, "v", "show the hash of the table")
	ap.SupportsFlag(systemFlag, "s", "show system tables")
	ap.SupportsFlag(allFlag, "a", "show system tables")
	return ap
}

// EventType returns the type of the event to log
func (cmd LsCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LS
}

// Exec executes the command
func (cmd LsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, lsDocumentation, ap)
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
		if !apr.Contains(systemFlag) || apr.Contains(allFlag) {
			verr = printUserTables(ctx, root, label, apr.Contains(verboseFlag))
			cli.Println()
		}

		if verr == nil && (apr.Contains(systemFlag) || apr.Contains(allFlag)) {
			verr = printSystemTables(ctx, root, dEnv.DoltDB, apr.Contains(verboseFlag))
			cli.Println()
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func getUserTableNames(root *doltdb.RootValue, ctx context.Context) ([]string, error) {
	tblNms, err := root.GetTableNames(ctx)

	if err != nil {
		return nil, err
	}

	tblNames := []string{}
	for _, name := range tblNms {
		if name != doltdb.DocTableName {
			tblNames = append(tblNames, name)
		}
	}

	sort.Strings(tblNames)

	return tblNames, nil
}

func printUserTables(ctx context.Context, root *doltdb.RootValue, label string, verbose bool) errhand.VerboseError {
	tblNames, err := getUserTableNames(root, ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to get tables").AddCause(err).Build()
	}

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

func printSystemTables(ctx context.Context, root *doltdb.RootValue, ddb *doltdb.DoltDB, verbose bool) errhand.VerboseError {
	tblNames, err := getUserTableNames(root, ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to get tables").AddCause(err).Build()
	}

	printWorkingSetSysTables(tblNames)

	if verbose {
		return printSysTablesNotInWorkingSet(err, ctx, ddb, tblNames)
	}

	return nil
}

func printWorkingSetSysTables(tblNames []string) {
	diffTables := funcitr.MapStrings(tblNames, func(s string) string { return sqle.DoltDiffTablePrefix + s })
	histTables := funcitr.MapStrings(tblNames, func(s string) string { return sqle.DoltHistoryTablePrefix + s })

	systemTables := []string{sqle.LogTableName, doltdb.DocTableName}
	systemTables = append(systemTables, diffTables...)
	systemTables = append(systemTables, histTables...)

	cli.Println("System tables:")
	cli.Println("\t" + strings.Join(systemTables, "\n\t"))
}

func printSysTablesNotInWorkingSet(err error, ctx context.Context, ddb *doltdb.DoltDB, tblNames []string) errhand.VerboseError {
	cmItr, err := doltdb.CommitItrForAllBranches(ctx, ddb)

	if err != nil {
		return errhand.BuildDError("error: failed to read history").AddCause(err).Build()
	}

	activeTableSet := set.NewStrSet(tblNames)
	deletedTableSet := set.NewStrSet([]string{})
	for {
		_, cm, err := cmItr.Next(ctx)

		if err == io.EOF {
			break
		} else if err != nil {
			return errhand.BuildDError("error: failed to iterate through history").AddCause(err).Build()
		}

		currRoot, err := cm.GetRootValue()

		if err != nil {
			return errhand.BuildDError("error: failed to read root from db.").AddCause(err).Build()
		}

		currTblNames, err := currRoot.GetTableNames(ctx)

		if err != nil {
			return errhand.BuildDError("error: failed to read tables").AddCause(err).Build()
		}

		_, missing := activeTableSet.IntersectAndMissing(currTblNames)
		deletedTableSet.Add(missing...)
	}

	if deletedTableSet.Size() > 0 {
		deletedSlice := deletedTableSet.AsSlice()
		sort.Strings(deletedSlice)

		const ncbPrefix = "(not on current branch) "
		diffTables := funcitr.MapStrings(deletedSlice, func(s string) string { return ncbPrefix + sqle.DoltDiffTablePrefix + s })
		histTables := funcitr.MapStrings(deletedSlice, func(s string) string { return ncbPrefix + sqle.DoltHistoryTablePrefix + s })

		systemTables := append(histTables, diffTables...)

		cli.Println("\t" + strings.Join(systemTables, "\n\t"))
	}

	return nil
}
