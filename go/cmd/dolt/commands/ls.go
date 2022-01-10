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
	"fmt"
	"io"
	"sort"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

const (
	systemFlag = "system"
)

var lsDocs = cli.CommandDocumentationContent{
	ShortDesc: "List tables",
	LongDesc: `With no arguments lists the tables in the current working set but if a commit is specified it will list the tables in that commit.  If the {{.EmphasisLeft}}--verbose{{.EmphasisRight}} flag is provided a row count and a hash of the table will also be displayed.

If the {{.EmphasisLeft}}--system{{.EmphasisRight}} flag is supplied this will show the dolt system tables which are queryable with SQL.  Some system tables can be queried even if they are not in the working set by specifying appropriate parameters in the SQL queries. To see these tables too you may pass the {{.EmphasisLeft}}--verbose{{.EmphasisRight}} flag.

If the {{.EmphasisLeft}}--all{{.EmphasisRight}} flag is supplied both user and system tables will be printed.
`,

	Synopsis: []string{
		"[--options] [{{.LessThan}}commit{{.GreaterThan}}]",
	},
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
func (cmd LsCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, lsDocs, ap))
}

func (cmd LsCmd) ArgParser() *argparser.ArgParser {
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
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, lsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.Contains(systemFlag) && apr.Contains(allFlag) {
		verr := errhand.BuildDError("--%s and --%s are mutually exclusive", systemFlag, allFlag).SetPrintUsage().Build()
		HandleVErrAndExitCode(verr, usage)
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

func getRootForCommitSpecStr(ctx context.Context, csStr string, dEnv *env.DoltEnv) (string, *doltdb.RootValue, errhand.VerboseError) {
	cs, err := doltdb.NewCommitSpec(csStr)

	if err != nil {
		bdr := errhand.BuildDError(`"%s" is not a validly formatted branch, or commit reference.`, csStr)
		return "", nil, bdr.AddCause(err).Build()
	}

	cm, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoStateReader().CWBHeadRef())

	if err != nil {
		return "", nil, errhand.BuildDError(`Unable to resolve "%s"`, csStr).AddCause(err).Build()
	}

	r, err := cm.GetRootValue()

	if err != nil {
		return "", nil, errhand.BuildDError("error: failed to get root").AddCause(err).Build()
	}

	h, err := cm.HashOf()

	if err != nil {
		return "", nil, errhand.BuildDError("error: failed to get commit hash").AddCause(err).Build()
	}

	return h.String(), r, nil
}

func printUserTables(ctx context.Context, root *doltdb.RootValue, label string, verbose bool) errhand.VerboseError {
	tblNames, err := doltdb.GetNonSystemTableNames(ctx, root)

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
			ls, verr := listTableVerbose(ctx, tbl, root)
			if verr != nil {
				return verr
			}
			cli.Println(ls)
		} else {
			cli.Println("\t", tbl)
		}
	}

	return nil
}

func listTableVerbose(ctx context.Context, tbl string, root *doltdb.RootValue) (string, errhand.VerboseError) {
	h, _, err := root.GetTableHash(ctx, tbl)

	if err != nil {
		return "", errhand.BuildDError("error: failed to get table hash").AddCause(err).Build()
	}

	tblVal, _, err := root.GetTable(ctx, tbl)

	if err != nil {
		return "", errhand.BuildDError("error: failed to get table").AddCause(err).Build()
	}

	rows, err := tblVal.GetNomsRowData(ctx)

	if err != nil {
		return "", errhand.BuildDError("error: failed to get row data").AddCause(err).Build()
	}

	return fmt.Sprintf("\t%-32s %s    %d rows\n", tbl, h.String(), rows.Len()), nil
}

func printSystemTables(ctx context.Context, root *doltdb.RootValue, ddb *doltdb.DoltDB, verbose bool) errhand.VerboseError {
	perSysTbls, err := doltdb.GetPersistedSystemTables(ctx, root)
	if err != nil {
		return errhand.BuildDError("error retrieving persisted table names").AddCause(err).Build()
	}

	genSysTbls, err := doltdb.GetGeneratedSystemTables(ctx, root)
	if err != nil {
		return errhand.BuildDError("error retrieving generated table names").AddCause(err).Build()
	}

	cli.Println("System tables:")
	for _, tbl := range perSysTbls {
		if verbose {
			ls, verr := listTableVerbose(ctx, tbl, root)
			if verr != nil {
				return verr
			}
			cli.Println(ls)
		} else {
			cli.Println("\t", tbl)
		}
	}
	for _, tbl := range genSysTbls {
		cli.Println("\t", tbl)
	}

	if verbose {
		return printSysTablesNotInWorkingSet(ctx, ddb, root)
	}

	return nil
}

func printSysTablesNotInWorkingSet(ctx context.Context, ddb *doltdb.DoltDB, root *doltdb.RootValue) errhand.VerboseError {
	workingSetTblNames, err := doltdb.GetAllTableNames(ctx, root)
	if err != nil {
		return errhand.BuildDError("failed to get table names").AddCause(err).Build()
	}
	activeTableSet := set.NewStrSet(workingSetTblNames)

	cmItr, err := doltdb.CommitItrForAllBranches(ctx, ddb)
	if err != nil {
		return errhand.BuildDError("error: failed to read history").AddCause(err).Build()
	}

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

		currTblNames, err := doltdb.GetSystemTableNames(ctx, currRoot)

		if err != nil {
			return errhand.BuildDError("error: failed to read tables").AddCause(err).Build()
		}

		_, _, missing := activeTableSet.LeftIntersectionRight(set.NewStrSet(currTblNames))
		deletedTableSet.Add(missing.AsSlice()...)
	}

	deletedSlice := deletedTableSet.AsSlice()
	sort.Strings(deletedSlice)
	for _, dt := range deletedSlice {
		const ncbPrefix = "(not on current branch) "
		cli.Printf("\t%s %s\n", ncbPrefix, dt)
	}

	return nil
}
