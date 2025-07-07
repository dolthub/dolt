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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

var branchDocs = cli.CommandDocumentationContent{
	ShortDesc: `List, create, or delete branches`,
	LongDesc: `If {{.EmphasisLeft}}--list{{.EmphasisRight}} is given, or if there are no non-option arguments, existing branches are listed. The current branch will be highlighted with an asterisk. With no options, only local branches are listed. With {{.EmphasisLeft}}-r{{.EmphasisRight}}, only remote branches are listed. With {{.EmphasisLeft}}-a{{.EmphasisRight}} both local and remote branches are listed. {{.EmphasisLeft}}-v{{.EmphasisRight}} causes the hash of the commit that the branches are at to be printed as well.

The command's second form creates a new branch head named {{.LessThan}}branchname{{.GreaterThan}} which points to the current {{.EmphasisLeft}}HEAD{{.EmphasisRight}}, or {{.LessThan}}start-point{{.GreaterThan}} if given.

Note that this will create the new branch, but it will not switch the working tree to it; use {{.EmphasisLeft}}dolt checkout <newbranch>{{.EmphasisRight}} to switch to the new branch.

With a {{.EmphasisLeft}}-m{{.EmphasisRight}}, {{.LessThan}}oldbranch{{.GreaterThan}} will be renamed to {{.LessThan}}newbranch{{.GreaterThan}}. If {{.LessThan}}newbranch{{.GreaterThan}} exists, -f must be used to force the rename to happen.

The {{.EmphasisLeft}}-c{{.EmphasisRight}} options have the exact same semantics as {{.EmphasisLeft}}-m{{.EmphasisRight}}, except instead of the branch being renamed it will be copied to a new name.

With a {{.EmphasisLeft}}-d{{.EmphasisRight}}, {{.LessThan}}branchname{{.GreaterThan}} will be deleted. You may specify more than one branch for deletion.`,
	Synopsis: []string{
		`[--list] [-v] [-a] [-r]`,
		`[-f] {{.LessThan}}branchname{{.GreaterThan}} [{{.LessThan}}start-point{{.GreaterThan}}]`,
		`-m [-f] [{{.LessThan}}oldbranch{{.GreaterThan}}] {{.LessThan}}newbranch{{.GreaterThan}}`,
		`-c [-f] [{{.LessThan}}oldbranch{{.GreaterThan}}] {{.LessThan}}newbranch{{.GreaterThan}}`,
		`-d [-f] [-r] {{.LessThan}}branchname{{.GreaterThan}}...`,
	},
}

const (
	datasetsFlag    = "datasets"
	showCurrentFlag = "show-current"
)

type BranchCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd BranchCmd) Name() string {
	return "branch"
}

// Description returns a description of the command
func (cmd BranchCmd) Description() string {
	return "Create, list, edit, delete branches."
}

func (cmd BranchCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(branchDocs, ap)
}

func (cmd BranchCmd) ArgParser() *argparser.ArgParser {
	// CreateBranchArgParser has the common flags for the command line and the stored procedure.
	// But only the command line has flags for printing branches. We define those here.
	ap := cli.CreateBranchArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"start-point", "A commit that a new branch should point at."})
	ap.SupportsFlag(cli.ListFlag, "", "List branches")
	ap.SupportsFlag(cli.VerboseFlag, "v", "When in list mode, show the hash and commit subject line for each head")
	ap.SupportsFlag(cli.AllFlag, "a", "When in list mode, shows remote tracked branches")
	ap.SupportsFlag(datasetsFlag, "", "List all datasets in the database")
	ap.SupportsFlag(cli.RemoteParam, "r", "When in list mode, show only remote tracked branches. When with -d, delete a remote tracking branch.")
	ap.SupportsFlag(showCurrentFlag, "", "Print the name of the current branch")
	return ap
}

// EventType returns the type of the event to log
func (cmd BranchCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_BRANCH
}

// Exec executes the command
func (cmd BranchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	apr, usage, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, branchDocs)
	if terminate {
		return status
	}

	errorBuilder := errhand.BuildDError("error: failed to create query engine")
	queryEngine, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errorBuilder.AddCause(err).Build(), nil)
	}

	if closeFunc != nil {
		defer closeFunc()
	}

	if len(apr.ContainsMany(cli.MoveFlag, cli.CopyFlag, cli.DeleteFlag, cli.DeleteForceFlag, cli.ListFlag, showCurrentFlag)) > 1 {
		cli.PrintErrln("Must specify exactly one of --move/-m, --copy/-c, --delete/-d, -D, --show-current, or --list.")
		return 1
	}

	switch {
	case apr.Contains(cli.MoveFlag):
		return moveBranch(sqlCtx, queryEngine, apr, args, usage)
	case apr.Contains(cli.CopyFlag):
		return copyBranch(sqlCtx, queryEngine, apr, args, usage)
	case apr.Contains(cli.DeleteFlag):
		return deleteBranches(sqlCtx, queryEngine, apr, args, usage)
	case apr.Contains(cli.DeleteForceFlag):
		return deleteBranches(sqlCtx, queryEngine, apr, args, usage)
	case apr.Contains(cli.ListFlag):
		return printBranches(sqlCtx, queryEngine, apr, usage)
	case apr.Contains(showCurrentFlag):
		return printCurrentBranch(sqlCtx, queryEngine)
	case apr.Contains(datasetsFlag):
		return printAllDatasets(sqlCtx, dEnv)
	case apr.ContainsAny(cli.SetUpstreamToFlag, cli.TrackFlag):
		return updateUpstream(sqlCtx, queryEngine, apr, args)
	case apr.NArg() > 0:
		return createBranch(sqlCtx, queryEngine, apr, args, usage)
	default:
		return printBranches(sqlCtx, queryEngine, apr, usage)
	}
}

type branchMeta struct {
	name   string
	hash   string
	remote bool
}

func getBranches(sqlCtx *sql.Context, queryEngine cli.Queryist, remote bool) ([]branchMeta, error) {
	var command string
	if remote {
		command = "SELECT name, hash from dolt_remote_branches"
	} else {
		command = "SELECT name, hash from dolt_branches"
	}

	schema, rowIter, _, err := queryEngine.Query(sqlCtx, command)
	if err != nil {
		return nil, err
	}

	var branches []branchMeta

	for {
		row, err := rowIter.Next(sqlCtx)
		if err == io.EOF {
			return branches, nil
		}
		if err != nil {
			return nil, err
		}
		if len(row) != 2 {
			return nil, fmt.Errorf("unexpectedly received multiple columns in '%s': %s", command, row)
		}

		rowStrings, err := sqlfmt.SqlRowAsStrings(sqlCtx, row, schema)
		if err != nil {
			return nil, err
		}

		branches = append(branches, branchMeta{name: rowStrings[0], hash: rowStrings[1], remote: remote})
	}
}

func printBranches(sqlCtx *sql.Context, queryEngine cli.Queryist, apr *argparser.ArgParseResults, _ cli.UsagePrinter) int {
	branchSet := set.NewStrSet(apr.Args)

	verbose := apr.Contains(cli.VerboseFlag)
	printRemote := apr.Contains(cli.RemoteParam)
	printAll := apr.Contains(cli.AllFlag)

	var branches []branchMeta
	if printAll || printRemote {
		remoteBranches, err := getBranches(sqlCtx, queryEngine, true)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error: failed to read remote branches from db").AddCause(err).Build(), nil)
		}
		branches = append(branches, remoteBranches...)
	}

	if printAll || !printRemote {
		localBranches, err := getBranches(sqlCtx, queryEngine, false)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("error: failed to read local branches from db").AddCause(err).Build(), nil)
		}
		branches = append(branches, localBranches...)
	}

	currentBranch, err := getActiveBranchName(sqlCtx, queryEngine)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to read current branch from db").AddCause(err).Build(), nil)
	}

	sort.Slice(branches, func(i, j int) bool {
		return branches[i].name < branches[j].name
	})

	for _, branch := range branches {
		if branchSet.Size() > 0 && !branchSet.Contains(branch.name) {
			continue
		}

		commitStr := ""
		branchName := "  " + branch.name
		branchLen := len(branchName)
		if branch.name == currentBranch {
			branchName = "* " + color.GreenString(branch.name)
		} else if branch.remote {
			branchName = "  " + color.RedString(branch.name)
		}

		if verbose {
			commitStr = branch.hash
		}

		// This silliness is requires to properly support color characters in branch names.
		fmtStr := fmt.Sprintf("%%s%%%ds\t%%s", 48-branchLen)
		line := fmt.Sprintf(fmtStr, branchName, "", commitStr)

		cli.Println(line)
	}

	return 0
}

func printCurrentBranch(sqlCtx *sql.Context, queryEngine cli.Queryist) int {
	currentBranchName, err := getActiveBranchName(sqlCtx, queryEngine)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to read current branch from db").AddCause(err).Build(), nil)
	}
	cli.Println(currentBranchName)
	return 0
}

func printAllDatasets(ctx context.Context, dEnv *env.DoltEnv) int {
	refs, err := dEnv.DoltDB(ctx).GetHeadRefs(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].String() < refs[j].String()
	})
	for _, r := range refs {
		cli.Println("  " + r.String())
	}

	branches, err := dEnv.DoltDB(ctx).GetBranches(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}
	sort.Slice(branches, func(i, j int) bool {
		return branches[i].String() < branches[j].String()
	})
	for _, b := range branches {
		var w ref.WorkingSetRef
		w, err = ref.WorkingSetRefForHead(b)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
		}

		_, err = dEnv.DoltDB(ctx).ResolveWorkingSet(ctx, w)
		if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
			continue
		} else if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
		}
		cli.Println("  " + w.String())
	}
	return 0
}

// generateBranchSql returns the query that will call the `DOLT_BRANCH` stored procedure.
func generateBranchSql(args []string) (string, error) {
	var buffer bytes.Buffer
	var first bool
	queryValues := make([]interface{}, 0, len(args))
	first = true
	buffer.WriteString("CALL DOLT_BRANCH(")

	for _, arg := range args {
		if !first {
			buffer.WriteString(", ")
		}
		first = false
		buffer.WriteString("?")
		queryValues = append(queryValues, arg)
	}
	buffer.WriteString(")")

	return dbr.InterpolateForDialect(buffer.String(), queryValues, dialect.MySQL)
}

func updateUpstream(sqlCtx *sql.Context, queryEngine cli.Queryist, apr *argparser.ArgParseResults, args []string) int {
	var branchName, remoteName string
	var err error
	if apr.NArg() == 0 {
		branchName, err = getActiveBranchName(sqlCtx, queryEngine)
		if err != nil {
			cli.PrintErrln("error: failed to get current branch from database")
			return 1
		}
	} else {
		branchName = apr.Arg(0)
	}

	if apr.Contains(cli.TrackFlag) && apr.Contains(cli.SetUpstreamToFlag) {
		cli.PrintErrln(fmt.Sprintf("error: --%s and --%s are mutually exclusive options.", cli.SetUpstreamToFlag, cli.TrackFlag))
		return 1
	} else if apr.Contains(cli.TrackFlag) {
		if apr.NArg() == 1 {
			remoteName, err = getActiveBranchName(sqlCtx, queryEngine)
			if err != nil {
				cli.PrintErrln("error: --track takes branch name and remote name")
				return 1
			}
		} else if apr.NArg() == 2 {
			remoteName = apr.Arg(1)
		} else {
			cli.PrintErrln("error: --track takes branch name and remote name")
			return 1
		}
	} else if apr.Contains(cli.SetUpstreamToFlag) {
		if apr.NArg() > 2 {
			cli.PrintErrln("error: --set-upstream-to takes branch name and remote name")
			return 1
		}
		remoteName, _ = apr.GetValue(cli.SetUpstreamToFlag)
	}

	res := callStoredProcedure(sqlCtx, queryEngine, args)
	if res != 0 {
		return res
	}
	cli.Printf("branch '%s' set up to track '%s'\n", branchName, remoteName)
	return 0
}

func createBranch(sqlCtx *sql.Context, queryEngine cli.Queryist, apr *argparser.ArgParseResults, args []string, usage cli.UsagePrinter) int {
	remoteName, useUpstream := apr.GetValue(cli.SetUpstreamToFlag)

	if apr.NArg() != 1 && apr.NArg() != 2 {
		usage()
		return 1
	}

	if apr.Contains(cli.AllFlag) {
		cli.PrintErrln("--all/-a can only be supplied when listing branches, not when creating branches")
		return 1
	}

	if apr.Contains(cli.VerboseFlag) {
		cli.PrintErrln("--verbose/-v can only be supplied when listing branches, not when creating branches")
		return 1
	}

	if apr.Contains(cli.RemoteParam) {
		cli.PrintErrln("--remote/-r can only be supplied when listing or deleting branches, not when creating branches")
		return 1
	}

	var branchName = apr.Arg(0)
	if !doltdb.IsValidUserBranchName(branchName) {
		cli.PrintErrf("%s is an invalid branch name", branchName)
		return 1
	}

	result := callStoredProcedure(sqlCtx, queryEngine, args)

	if result != 0 {
		return result
	}

	if useUpstream {
		cli.Printf("branch '%s' set up to track '%s'\n", apr.Arg(0), remoteName)
	}

	return 0
}

func moveBranch(sqlCtx *sql.Context, queryEngine cli.Queryist, apr *argparser.ArgParseResults, args []string, usage cli.UsagePrinter) int {
	if apr.NArg() != 1 && apr.NArg() != 2 {
		usage()
		return 1
	}

	if apr.Contains(cli.AllFlag) {
		cli.PrintErrln("--all/-a can only be supplied when listing branches, not when moving branches")
		return 1
	}

	if apr.Contains(cli.VerboseFlag) {
		cli.PrintErrln("--verbose/-v can only be supplied when listing branches, not when moving branches")
		return 1
	}

	if apr.Contains(cli.RemoteParam) {
		cli.PrintErrln("--remote/-r can only be supplied when listing or deleting branches, not when moving branches")
		return 1
	}

	var newName = apr.Arg(0)
	if apr.NArg() == 2 {
		newName = apr.Arg(1)
	}
	if !doltdb.IsValidUserBranchName(newName) {
		cli.PrintErrf("%s is an invalid branch name", newName)
		return 1
	}

	return callStoredProcedure(sqlCtx, queryEngine, args)
}

func copyBranch(sqlCtx *sql.Context, queryEngine cli.Queryist, apr *argparser.ArgParseResults, args []string, usage cli.UsagePrinter) int {
	if apr.NArg() != 1 && apr.NArg() != 2 {
		usage()
		return 1
	}

	if apr.Contains(cli.AllFlag) {
		cli.PrintErrln("--all/-a can only be supplied when listing branches, not when copying branches")
		return 1
	}

	if apr.Contains(cli.VerboseFlag) {
		cli.PrintErrln("--verbose/-v can only be supplied when listing branches, not when copying branches")
		return 1
	}

	if apr.Contains(cli.RemoteParam) {
		cli.PrintErrln("--remote/-r can only be supplied when listing or deleting branches, not when copying branches")
		return 1
	}

	var toName = apr.Arg(0)
	if apr.NArg() == 2 {
		toName = apr.Arg(1)
	}
	if !doltdb.IsValidUserBranchName(toName) {
		cli.PrintErrf("%s is an invalid branch name", toName)
		return 1
	}

	return callStoredProcedure(sqlCtx, queryEngine, args)
}

func deleteBranches(sqlCtx *sql.Context, queryEngine cli.Queryist, apr *argparser.ArgParseResults, args []string, usage cli.UsagePrinter) int {
	if apr.NArg() == 0 {
		usage()
		return 1
	}

	if apr.Contains(cli.AllFlag) {
		cli.PrintErrln("--all/-a can only be supplied when listing branches, not when deleting branches")
		return 1
	}

	if apr.Contains(cli.VerboseFlag) {
		cli.PrintErrln("--verbose/-v can only be supplied when listing branches, not when deleting branches")
		return 1
	}

	return callStoredProcedure(sqlCtx, queryEngine, args)
}

func generateForceDeleteMessage(args []string) string {
	newArgs := ""
	for _, arg := range args {
		if arg != "--force" && arg != "-f" && arg != "-D" && arg != "-d" {
			newArgs = newArgs + " " + arg
		}
	}
	return newArgs
}

// callStoredProcedure generates and executes the SQL query for calling the DOLT_BRANCH stored procedure.
// All actions that modify branches delegate to this after they validate their arguments.
// Actions that don't modify branches, such as `dolt branch --list` and `dolt branch --show-current`, don't call
// this method.
func callStoredProcedure(sqlCtx *sql.Context, queryEngine cli.Queryist, args []string) int {
	query, err := generateBranchSql(args)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), nil)
	}
	_, rowIter, _, err := queryEngine.Query(sqlCtx, query)
	if err != nil {
		if strings.Contains(err.Error(), "is not fully merged") {
			newErrorMessage := fmt.Sprintf("%s. If you are sure you want to delete it, run 'dolt branch -D%s'", err.Error(), generateForceDeleteMessage(args))
			return HandleVErrAndExitCode(errhand.BuildDError("%s", newErrorMessage).Build(), nil)
		}
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(fmt.Errorf("error: %s", err.Error())), nil)
	}
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("error: failed to get result rows for query %s", query).AddCause(err).Build(), nil)
	}

	return 0
}

// BuildVerrAndExit is a shortcut for building a verbose error and calling HandleVerrAndExitCode with it
func BuildVerrAndExit(errMsg string, cause error) int {
	return HandleVErrAndExitCode(errhand.BuildDError("%s", errMsg).AddCause(cause).Build(), nil)
}
