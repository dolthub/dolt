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
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/util/outputpager"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
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
	"to add the tables to the staged set, and `dolt cherry-pick --continue` to complete the cherry-pick. \n" +
	"To undo all changes from this cherry-pick operation, use `dolt cherry-pick --abort`.\n" +
	"For more information on handling conflicts, see: https://docs.dolthub.com/concepts/dolt/git/conflicts")

var ErrCherryPickVerificationFailed = errors.NewKind("error: Commit verification failed. Your changes are staged " +
	"in the working set. Fix the failing tests, use `dolt add` to stage your changes, then " +
	"`dolt cherry-pick --continue` to complete the cherry-pick.\n" +
	"To undo all changes from this cherry-pick operation, use `dolt cherry-pick --abort`.\n" +
	"Run `dolt sql -q 'select * from dolt_test_run()` to see which tests are failing.")

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

	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	// dolt_cherry_pick performs this check as well. Check performed early here to short circuit the operation.
	err = branch_control.CheckAccess(queryist.Context, branch_control.Permissions_Write)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	// Check for mutually exclusive flags
	if apr.Contains(cli.AbortParam) && apr.Contains(cli.ContinueFlag) {
		err = fmt.Errorf("error: --continue and --abort are mutually exclusive")
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if apr.Contains(cli.AbortParam) {
		err = cherryPickAbort(queryist.Context, queryist.Queryist)
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if apr.Contains(cli.ContinueFlag) {
		err = cherryPickContinue(queryist.Context, queryist.Queryist)
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if apr.Contains(cli.NoJsonMergeFlag) {
		_, _, _, err = queryist.Queryist.Query(queryist.Context, "set @@session.dolt_dont_merge_json = 1")
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

	err = cherryPick(queryist.Context, queryist.Queryist, apr, args)
	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}

func cherryPick(sqlCtx *sql.Context, queryist cli.Queryist, apr *argparser.ArgParseResults, args []string) error {
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

	_, err = cli.GetRowsForSql(queryist, sqlCtx, "set @@dolt_allow_commit_conflicts = 1")
	if err != nil {
		return fmt.Errorf("error: failed to set @@dolt_allow_commit_conflicts: %w", err)
	}

	_, err = cli.GetRowsForSql(queryist, sqlCtx, "set @@dolt_force_transaction_commit = 1")
	if err != nil {
		return fmt.Errorf("error: failed to set @@dolt_force_transaction_commit: %w", err)
	}

	q, err := interpolateStoredProcedureCall("DOLT_CHERRY_PICK", args)
	if err != nil {
		return fmt.Errorf("error: failed to interpolate query: %w", err)
	}
	rows, err := cli.GetRowsForSql(queryist, sqlCtx, q)
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
	verificationFailed := false
	commitHash := ""
	for _, row := range rows {
		var ok bool
		commitHash, ok, err = sql.Unwrap[string](sqlCtx, row[0])
		if err != nil {
			return fmt.Errorf("Unable to parse commitHash column: %w", err)
		}
		if !ok {
			return fmt.Errorf("Unexpected type for commitHash column, expected string, found %T", row[0])
		}
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
		verificationFailures, err := getInt64ColAsInt64(row[4])
		if err != nil {
			return fmt.Errorf("Unable to parse verification_failures column: %w", err)
		}

		if verificationFailures > 0 {
			verificationFailed = true
		}

		// if we have a hash and all 0s, then the cherry-pick succeeded
		if len(commitHash) > 0 && dataConflicts == 0 && schemaConflicts == 0 && constraintViolations == 0 && verificationFailures == 0 {
			succeeded = true
		}
	}

	if succeeded {
		// on success, print the commit info
		commit, err := getCommitInfo(sqlCtx, queryist, commitHash)
		if commit == nil || err != nil {
			return fmt.Errorf("error: failed to get commit metadata for ref '%s': %v", commitHash, err)
		}

		cli.ExecuteWithStdioRestored(func() {
			pager := outputpager.Start()
			defer pager.Stop()

			PrintCommitInfo(pager, 0, false, false, "auto", commit)
		})

		return nil
	} else if verificationFailed {
		return ErrCherryPickVerificationFailed.New()
	} else {
		// this failure could only have been caused by constraint violations or conflicts during cherry-pick
		return ErrCherryPickConflictsOrViolations.New()
	}
}

func cherryPickAbort(sqlCtx *sql.Context, queryist cli.Queryist) error {
	query := "call dolt_cherry_pick('--abort')"
	_, err := cli.GetRowsForSql(queryist, sqlCtx, query)
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

func cherryPickContinue(sqlCtx *sql.Context, queryist cli.Queryist) error {
	query := "call dolt_cherry_pick('--continue')"
	rows, err := cli.GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		return err
	}

	if len(rows) != 1 {
		return fmt.Errorf("error: unexpected number of rows returned from dolt_cherry_pick: %d", len(rows))
	}

	// No version of dolt_cherry_pick has ever returned less than 4 columns. We don't set an upper bound here to
	// allow for servers to add more columns in the future without breaking compatibility with older clients.
	if len(rows[0]) < 4 {
		return fmt.Errorf("error: unexpected number of columns returned from dolt_cherry_pick: %d", len(rows[0]))
	}

	row := rows[0]

	// We expect to get an error if there were problems, but we also could get any of the conflicts and
	// violation counts being greater than 0 if there were problems. If we got here without an error,
	// but we have conflicts, violations, or verification failures, we should report and stop.
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
	verificationFailures := int64(0)
	if len(row) > 4 {
		// verification failure column was added to results briefly after the commit_verification feature was added.
		// Original version returned an error. This really only matters is the dolt version of the cli is different from
		// the version of the server it connected to.
		verificationFailures, err = getInt64ColAsInt64(row[4])
		if err != nil {
			return fmt.Errorf("Unable to parse verification_failures column: %w", err)
		}
	}
	if dataConflicts > 0 || schemaConflicts > 0 || constraintViolations > 0 {
		return ErrCherryPickConflictsOrViolations.New()
	}
	if verificationFailures > 0 {
		return ErrCherryPickVerificationFailed.New()
	}

	commitHash := fmt.Sprintf("%v", row[0])

	commit, err := getCommitInfo(sqlCtx, queryist, commitHash)
	if commit == nil || err != nil {
		return fmt.Errorf("error: failed to get commit metadata for ref '%s': %v", commitHash, err)
	}

	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		PrintCommitInfo(pager, 0, false, false, "auto", commit)
	})

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
