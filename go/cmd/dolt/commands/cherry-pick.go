// Copyright 2022 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

var cherryPickDocs = cli.CommandDocumentationContent{
	ShortDesc: `Apply the changes introduced by an existing commit.`,
	LongDesc: `
Applies the changes from an existing commit and creates a new commit from the current HEAD. This requires your working tree to be clean (no modifications from the HEAD commit).

Cherry-picking merge commits or commits with table drops/renames is not currently supported. 

If any data conflicts, schema conflicts, or constraint violations are detected during cherry-picking, you can use Dolt's conflict resolution features to resolve them. For more information on resolving conflicts, see: https://docs.dolthub.com/concepts/dolt/git/conflicts.
`,
	Synopsis: []string{
		`[--allow-empty] {{.LessThan}}commit{{.GreaterThan}}`,
	},
}

var ErrCherryPickConflictsOrViolations = errors.NewKind("error: Unable to apply commit cleanly due to conflicts " +
	"or constraint violations. Please resolve the conflicts and/or constraint violations, then use `dolt add` " +
	"to add the tables to the staged set, and `dolt commit` to commit the changes and finish cherry-picking. \n" +
	"To undo all changes from this cherry-pick operation, use `dolt cherry-pick --abort`.\n" +
	"For more information on handling conflicts, see: https://docs.dolthub.com/concepts/dolt/git/conflicts")

type CherryPickCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command.
func (cmd CherryPickCmd) Name() string {
	return "cherry-pick"
}

// Description returns a description of the command.
func (cmd CherryPickCmd) Description() string {
	return "Apply the changes introduced by an existing commit."
}

func (cmd CherryPickCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCherryPickArgParser()
	return cli.NewCommandDocumentation(cherryPickDocs, ap)
}

func (cmd CherryPickCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCherryPickArgParser()
}

// EventType returns the type of the event to log.
func (cmd CherryPickCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHERRY_PICK
}

// Exec executes the command.
func (cmd CherryPickCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateCherryPickArgParser()
	ap.SupportsFlag(cli.NoJsonMergeFlag, "", "Do not attempt to automatically resolve multiple changes to the same JSON value, report a conflict instead.")
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cherryPickDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	// dolt_cherry_pick performs this check as well. Check performed early here to short circuit the operation.
	err = branch_control.CheckAccess(sqlCtx, branch_control.Permissions_Write)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	if apr.Contains(cli.AbortParam) {
		err = cherryPickAbort(queryist, sqlCtx)
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if apr.Contains(cli.NoJsonMergeFlag) {
		_, _, _, err = queryist.Query(sqlCtx, "set @@session.dolt_dont_merge_json = 1")
		if err != nil {
			cli.Println(err.Error())
			return 1
		}
	}

	// TODO : support single commit cherry-pick only for now
	if apr.NArg() == 0 {
		usage()
		return 1
	} else if apr.NArg() > 1 {
		return HandleVErrAndExitCode(errhand.BuildDError("cherry-picking multiple commits is not supported yet").SetPrintUsage().Build(), usage)
	}

	err = cherryPick(queryist, sqlCtx, apr, args)
	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}

func cherryPick(queryist cli.Queryist, sqlCtx *sql.Context, apr *argparser.ArgParseResults, args []string) error {
	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		return fmt.Errorf("error: cannot cherry-pick empty string")
	}

	hasStagedChanges, hasUnstagedChanges, err := hasStagedAndUnstagedChanged(queryist, sqlCtx)
	if err != nil {
		return fmt.Errorf("error: failed to check for staged and unstaged changes: %w", err)
	}
	if hasStagedChanges {
		return fmt.Errorf("Please commit your staged changes before using cherry-pick.")
	}
	if hasUnstagedChanges {
		return fmt.Errorf(`error: your local changes would be overwritten by cherry-pick.
hint: commit your changes (dolt commit -am \"<message>\") or reset them (dolt reset --hard) to proceed.`)
	}

	_, err = GetRowsForSql(queryist, sqlCtx, "set @@dolt_allow_commit_conflicts = 1")
	if err != nil {
		return fmt.Errorf("error: failed to set @@dolt_allow_commit_conflicts: %w", err)
	}

	_, err = GetRowsForSql(queryist, sqlCtx, "set @@dolt_force_transaction_commit = 1")
	if err != nil {
		return fmt.Errorf("error: failed to set @@dolt_force_transaction_commit: %w", err)
	}

	q, err := interpolateStoredProcedureCall("DOLT_CHERRY_PICK", args)
	if err != nil {
		return fmt.Errorf("error: failed to interpolate query: %w", err)
	}
	rows, err := GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		errorText := err.Error()
		switch {
		case strings.Contains("nothing to commit", errorText):
			cli.Println("No changes were made.")
			return nil
		default:
			return err
		}
	}

	if len(rows) != 1 {
		return fmt.Errorf("error: unexpected number of rows returned from dolt_cherry_pick: %d", len(rows))
	}

	succeeded := false
	commitHash := ""
	for _, row := range rows {
		commitHash = row[0].(string)
		dataConflicts, err := getInt64ColAsInt64(row[1])
		if err != nil {
			return fmt.Errorf("Unable to parse data_conflicts column: %w", err)
		}
		schemaConflicts, err := getInt64ColAsInt64(row[2])
		if err != nil {
			return fmt.Errorf("Unable to parse schema_conflicts column: %w", err)
		}
		constraintViolations, err := getInt64ColAsInt64(row[3])
		if err != nil {
			return fmt.Errorf("Unable to parse constraint_violations column: %w", err)
		}

		// if we have a hash and all 0s, then the cherry-pick succeeded
		if len(commitHash) > 0 && dataConflicts == 0 && schemaConflicts == 0 && constraintViolations == 0 {
			succeeded = true
		}
	}

	if succeeded {
		// on success, print the commit info
		commit, err := getCommitInfo(queryist, sqlCtx, commitHash)
		if commit == nil || err != nil {
			return fmt.Errorf("error: failed to get commit metadata for ref '%s': %v", commitHash, err)
		}

		cli.ExecuteWithStdioRestored(func() {
			pager := outputpager.Start()
			defer pager.Stop()

			PrintCommitInfo(pager, 0, false, false, "auto", commit)
		})

		return nil
	} else {
		// this failure could only have been caused by constraint violations or conflicts during cherry-pick
		return ErrCherryPickConflictsOrViolations.New()
	}
}

func cherryPickAbort(queryist cli.Queryist, sqlCtx *sql.Context) error {
	query := "call dolt_cherry_pick('--abort')"
	_, err := GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		errorText := err.Error()
		switch errorText {
		case "fatal: There is no merge to abort":
			return fmt.Errorf("error: There is no cherry-pick merge to abort")
		default:
			return err
		}
	}
	return nil
}

func hasStagedAndUnstagedChanged(queryist cli.Queryist, sqlCtx *sql.Context) (hasStagedChanges bool, hasUnstagedChanges bool, err error) {
	stagedTables, unstagedTables, err := GetDoltStatus(queryist, sqlCtx)
	if err != nil {
		return false, false, fmt.Errorf("error: failed to get dolt status: %w", err)
	}

	hasStagedChanges = len(stagedTables) > 0
	hasUnstagedChanges = len(unstagedTables) > 0
	return hasStagedChanges, hasUnstagedChanges, nil
}
