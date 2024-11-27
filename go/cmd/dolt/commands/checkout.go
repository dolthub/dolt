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
	"fmt"
	"strings"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var checkoutDocs = cli.CommandDocumentationContent{
	ShortDesc: `Switch branches or restore working tree tables`,
	LongDesc: `
Updates tables in the working set to match the staged versions. If no paths are given, dolt checkout will also update HEAD to set the specified branch as the current branch.

dolt checkout {{.LessThan}}branch{{.GreaterThan}}
   To prepare for working on {{.LessThan}}branch{{.GreaterThan}}, switch to it by updating the index and the tables in the working tree, and by pointing HEAD at the branch. Local modifications to the tables in the working
   tree are kept, so that they can be committed to the {{.LessThan}}branch{{.GreaterThan}}.

dolt checkout {{.LessThan}}commit{{.GreaterThan}} [--] {{.LessThan}}table{{.GreaterThan}}...
	 Specifying table names after a commit reference (branch, commit hash, tag, etc.) updates the working set to match that commit for one or more tables, but keeps the current branch. Local modifications to the tables named will be overwritten by their versions in the commit named.

dolt checkout -b {{.LessThan}}new_branch{{.GreaterThan}} [{{.LessThan}}start_point{{.GreaterThan}}]
   Specifying -b causes a new branch to be created as if dolt branch were called and then checked out.

dolt checkout {{.LessThan}}table{{.GreaterThan}}...
  To update table(s) with their values in HEAD `,
	Synopsis: []string{
		`{{.LessThan}}branch{{.GreaterThan}}`,
		`{{.LessThan}}commit{{.GreaterThan}} [--] {{.LessThan}}table{{.GreaterThan}}...`,
		`{{.LessThan}}table{{.GreaterThan}}...`,
		`-b {{.LessThan}}new-branch{{.GreaterThan}} [{{.LessThan}}start-point{{.GreaterThan}}]`,
		`--track {{.LessThan}}remote{{.GreaterThan}}/{{.LessThan}}branch{{.GreaterThan}}`,
	},
}

type CheckoutCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CheckoutCmd) Name() string {
	return "checkout"
}

// Description returns a description of the command
func (cmd CheckoutCmd) Description() string {
	return "Checkout a branch or overwrite a table from HEAD."
}

func (cmd CheckoutCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCheckoutArgParser()
	return cli.NewCommandDocumentation(checkoutDocs, ap)
}

func (cmd CheckoutCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCheckoutArgParser()
}

// EventType returns the type of the event to log
func (cmd CheckoutCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHECKOUT
}

// Exec executes the command
func (cmd CheckoutCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateCheckoutArgParser()
	apr, usage, terminate, status := ParseArgsOrPrintHelp(ap, commandStr, args, checkoutDocs)
	if terminate {
		return status
	}

	queryEngine, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()

		// We only check for this case when checkout is the first command in a session. The reason for this is that checkout
		// when connected to a remote server will not work as it won't set the branch. But when operating within the context
		// of another session, specifically a \checkout in a dolt sql session, this makes sense. Since no closeFunc would be
		// returned, we don't need to check for this case.
		_, ok := queryEngine.(*engine.SqlEngine)
		if !ok {
			msg := fmt.Sprintf(cli.RemoteUnsupportedMsg, commandStr)
			cli.Println(msg)
			return 1
		}
	}

	// Argument validation in the CLI is strictly nice to have. The stored procedure will do the same, but the errors
	// won't be as nice.
	branchOrTrack := apr.Contains(cli.CheckoutCreateBranch) || apr.Contains(cli.CreateResetBranch) || apr.Contains(cli.TrackFlag)
	if (branchOrTrack && apr.NArg() > 1) || (!branchOrTrack && apr.NArg() == 0) {
		usage()
		return 1
	}

	// Branch name retrieval here is strictly for messages. dolt_checkout procedure is the authority on logic around validation.
	var branchName string
	if apr.Contains(cli.CheckoutCreateBranch) {
		branchName, _ = apr.GetValue(cli.CheckoutCreateBranch)
	} else if apr.Contains(cli.CreateResetBranch) {
		branchName, _ = apr.GetValue(cli.CreateResetBranch)
	} else if apr.Contains(cli.TrackFlag) {
		if apr.NArg() > 0 {
			usage()
			return 1
		}
		remoteAndBranchName, _ := apr.GetValue(cli.TrackFlag)
		_, branchName = actions.ParseRemoteBranchName(remoteAndBranchName)
	} else if apr.NArg() > 0 {
		branchName = apr.Arg(0)
	}

	sqlQuery, err := generateCheckoutSql(args)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	rows, err := GetRowsForSql(queryEngine, sqlCtx, sqlQuery)

	if err != nil {
		// In fringe cases the server can't start because the default branch doesn't exist, `dolt checkout <existing branch>`
		// offers an escape hatch.
		if dEnv != nil {
			// TODO - This error handling is not great.
			if !branchOrTrack && strings.Contains(err.Error(), "cannot resolve default branch head for database") {
				err := saveHeadBranch(dEnv.FS, branchName)
				if err != nil {
					cli.PrintErr(err)
					return 1
				}
				return 0
			}
		}
		return HandleVErrAndExitCode(handleErrors(branchName, err), usage)
	}

	if len(rows) != 1 {
		return HandleVErrAndExitCode(errhand.BuildDError("expected 1 row response from %s, got %d", sqlQuery, len(rows)).Build(), usage)
	}

	if rows[0].Len() < 2 {
		return HandleVErrAndExitCode(errhand.BuildDError("no 'message' field in response from %s", sqlQuery).Build(), usage)
	}

	message, ok := rows[0].GetValue(1).(string)
	if !ok {
		return HandleVErrAndExitCode(errhand.BuildDError("expected string value for 'message' field in response from %s ", sqlQuery).Build(), usage)
	}

	if message != "" {
		cli.Println(message)
	}

	if dEnv != nil {
		if strings.Contains(message, "Switched to branch") {
			err := saveHeadBranch(dEnv.FS, branchName)
			if err != nil {
				cli.PrintErr(err)
				return 1
			}
			// This command doesn't modify `dEnv` which could break tests that call multiple commands in sequence.
			// We must reload it so that it includes changes to the repo state.
			err = dEnv.ReloadRepoState()
			if err != nil {
				return 1
			}
		}
	}

	return 0
}

// generateCheckoutSql returns the query that will call the `DOLT_CHECKOUT` stored procedure.
func generateCheckoutSql(args []string) (string, error) {
	var buffer bytes.Buffer
	queryValues := make([]interface{}, 0, len(args))

	buffer.WriteString("CALL DOLT_CHECKOUT('--move'")

	for _, arg := range args {
		buffer.WriteString(", ?")
		queryValues = append(queryValues, arg)
	}
	buffer.WriteString(")")

	return dbr.InterpolateForDialect(buffer.String(), queryValues, dialect.MySQL)
}

func handleErrors(branchName string, err error) errhand.VerboseError {
	if err.Error() == doltdb.ErrBranchNotFound.Error() {
		return errhand.BuildDError("fatal: Branch '%s' not found.", branchName).Build()
	} else if strings.Contains(err.Error(), "dolt does not support a detached head state.") {
		return errhand.VerboseErrorFromError(err)
	} else if strings.Contains(err.Error(), "error: could not find") {
		return errhand.VerboseErrorFromError(err)
	} else if doltdb.IsRootValUnreachable(err) {
		return errhand.VerboseErrorFromError(err)
	} else if actions.IsCheckoutWouldOverwrite(err) {
		return errhand.VerboseErrorFromError(err)
	} else if err.Error() == actions.ErrWorkingSetsOnBothBranches.Error() {
		str := fmt.Sprintf("error: There are uncommitted changes already on branch '%s'.", branchName) +
			"This can happen when someone modifies that branch in a SQL session." +
			fmt.Sprintf("You have uncommitted changes on this branch, and they would overwrite the uncommitted changes on branch %s on checkout.", branchName) +
			"To solve this problem, you can " +
			"1) commit or reset your changes on this branch, using `dolt commit` or `dolt reset`, before checking out the other branch, " +
			"2) use the `-f` flag with `dolt checkout` to force an overwrite, or " +
			"3) connect to branch '%s' with the SQL server and revert or commit changes there before proceeding."
		return errhand.BuildDError(str).AddCause(err).Build()
	} else {
		bdr := errhand.BuildDError("fatal: Unexpected error checking out branch '%s'", branchName)
		bdr.AddCause(err)
		return bdr.Build()
	}
}

func saveHeadBranch(fs filesys.ReadWriteFS, headBranch string) error {
	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}
	repoState.Head.Ref = ref.NewBranchRef(headBranch)
	return repoState.Save(fs)
}
