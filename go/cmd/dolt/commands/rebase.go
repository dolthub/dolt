// Copyright 2024 Dolthub, Inc.
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
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/editor"
)

var rebaseDocs = cli.CommandDocumentationContent{
	ShortDesc: "Reapplies commits on top of another base tip",
	LongDesc: `Rewrites commit history for the current branch by replaying commits, allowing the commits to be reordered, 
squashed, or dropped. The commits included in the rebase plan are the commits reachable by the current branch, but NOT 
reachable from the branch specified as the argument when starting a rebase (also known as the upstream branch). This is 
the same as Git and Dolt's ["two dot log" syntax](https://www.dolthub.com/blog/2022-11-11-two-and-three-dot-diff-and-log/#two-dot-log), 
or |upstreamBranch|..|currentBranch|.

For example, consider the commit graph below, where a {{.EmphasisLeft}}feature{{.EmphasisRight}} branch has branched off of a {{.EmphasisLeft}}main{{.EmphasisRight}} branch, and both branches have added commits:

	A → B → C → D → E → F  main
			 ↘
			  G → H → I  feature

If we rebase from the {{.EmphasisLeft}}feature{{.EmphasisRight}} branch using the {{.EmphasisLeft}}main{{.EmphasisRight}} branch as our upstream, the default rebase plan will include commits {{.EmphasisLeft}}G{{.EmphasisRight}}, {{.EmphasisLeft}}H{{.EmphasisRight}}, and {{.EmphasisLeft}}I{{.EmphasisRight}}, since those commits are reachable from our current branch, but NOT reachable from the upstream branch. By default, the changes from those same commits will be reapplied, in the same order, to the tip of the upstream branch {{.EmphasisLeft}}main{{.EmphasisRight}}. The resulting commit graph will then look like:
	A → B → C → D → E → F  main
						 ↘
						  G' → H' → I'  feature

Rebasing is useful to clean and organize your commit history, especially before merging a feature branch back to a shared branch. For example, you can drop commits that contain debugging or test changes, or squash or fixup small commits into a single commit, or reorder commits so that related changes are adjacent in the new commit history.

	CALL DOLT_REBASE('--interactive', 'main');
	CALL DOLT_REBASE('-i', 'main');
	CALL DOLT_REBASE('--continue');
	CALL DOLT_REBASE('--abort');
`,
	Synopsis: []string{
		`-i | --interactive [--continue] [--abort]`,
	},
}

type RebaseCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RebaseCmd) Name() string {
	return "rebase"
}

// Description returns a description of the command
func (cmd RebaseCmd) Description() string {
	return rebaseDocs.ShortDesc
}

// EventType returns the type of the event to log
func (cmd RebaseCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_REBASE
}

func (cmd RebaseCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(rebaseDocs, ap)
}

func (cmd RebaseCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateRebaseArgParser()
}

func (cmd RebaseCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd RebaseCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, rebaseDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	branchName, err := getActiveBranchName(sqlCtx, queryist)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	rebaseBranchName := "dolt_rebase_" + branchName

	query, err := constructInterpolatedDoltRebaseQuery(apr, rebaseBranchName)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	rows, err := GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	status, err := getInt64ColAsInt64(rows[0][0])
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if status == 1 {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("error: "+rows[0][1].(string))), usage)
	}

	message := rows[0][1].(string)
	if strings.Contains(message, "interactive rebase completed") {
		cli.Println("Successfully rebased and updated refs/heads/" + branchName + ".")
	} else if strings.Contains(message, "interactive rebase aborted") {
		cli.Println("Interactive rebase aborted.")
	} else {
		rebasePlan, err := getRebasePlan(cliCtx, sqlCtx, queryist, apr.Arg(0), branchName)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}

		if rebasePlan == nil || rebasePlan.Steps == nil {
			rows, err := GetRowsForSql(queryist, sqlCtx, "CALL DOLT_REBASE('--abort');")
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			status, err := getInt64ColAsInt64(rows[0][0])
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			if status == 1 {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("error: "+rows[0][1].(string))), usage)
			}

			cli.Println("Interactive rebase aborted.")
		} else {
			err = insertRebasePlanIntoDoltRebaseTable(rebasePlan, sqlCtx, queryist)
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}

			rows, err := GetRowsForSql(queryist, sqlCtx, "CALL DOLT_REBASE('--continue');")
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			status, err := getInt64ColAsInt64(rows[0][0])
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
			if status == 1 {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("error: "+rows[0][1].(string))), usage)
			}

			cli.Println("Successfully rebased and updated refs/heads/" + branchName + ".")
		}
	}

	return HandleVErrAndExitCode(nil, usage)
}

// constructInterpolatedDoltRebaseQuery generates the sql query necessary to call the DOLT_REBASE() function.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltRebaseQuery(apr *argparser.ArgParseResults, rebaseBranchName string) (string, error) {
	var params []interface{}
	var args []string
	var checkoutQuery string

	if apr.NArg() == 1 {
		params = append(params, apr.Arg(0))
		args = append(args, "?")
	}
	if apr.Contains(cli.InteractiveFlag) {
		args = append(args, "'--interactive'")
	}
	if apr.Contains(cli.ContinueFlag) {
		args = append(args, "'--continue'")
	}
	if apr.Contains(cli.AbortParam) {
		args = append(args, "'--abort'")
	}

	/*if apr.ContainsAny(cli.ContinueFlag, cli.AbortParam) {
		checkoutQuery = fmt.Sprintf("CALL DOLT_CHECKOUT('dolt_rebase_%s'); ", rebaseBranchName)
	}*/

	query := fmt.Sprintf("%sCALL DOLT_REBASE(%s);", checkoutQuery, strings.Join(args, ", "))
	interpolatedQuery, err := dbr.InterpolateForDialect(query, params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}

// getRebasePlan opens an editor for users to edit the rebase plan and returns the parsed rebase plan from the editor.
func getRebasePlan(cliCtx cli.CliContext, sqlCtx *sql.Context, queryist cli.Queryist, rebaseBranch, currentBranch string) (*rebase.RebasePlan, error) {
	if cli.ExecuteWithStdioRestored == nil {
		return nil, nil
	}

	if !checkIsTerminal() {
		return nil, nil
	}

	initialRebaseMsg, err := buildInitialRebaseMsg(sqlCtx, queryist, rebaseBranch, currentBranch)
	if err != nil {
		return nil, err
	}

	backupEd := "vim"
	// try getting default editor on the user system
	if ed, edSet := os.LookupEnv(dconfig.EnvEditor); edSet {
		backupEd = ed
	}
	// try getting Dolt config core.editor
	editorStr := cliCtx.Config().GetStringOrDefault(config.DoltEditor, backupEd)

	var rebaseMsg string
	cli.ExecuteWithStdioRestored(func() {
		rebaseMsg, err = editor.OpenTempEditor(editorStr, initialRebaseMsg)
	})
	if err != nil {
		return nil, err
	}

	rebasePlan, err := parseRebaseMessage(rebaseMsg)
	if err != nil {
		return nil, err
	}

	return rebasePlan, nil
}

// buildInitialRebaseMsg builds the initial message to display to the user when they open the rebase plan editor,
// including the formatted rebase plan.
func buildInitialRebaseMsg(sqlCtx *sql.Context, queryist cli.Queryist, rebaseBranch, currentBranch string) (string, error) {
	var buffer bytes.Buffer

	rows, err := GetRowsForSql(queryist, sqlCtx, "SELECT action, commit_hash, commit_message FROM dolt_rebase ORDER BY rebase_order")
	if err != nil {
		return "", err
	}

	// rebase plan
	for _, row := range rows {
		action, found := getRebaseAction(row[0])
		if !found {
			return "", errors.New("invalid rebase action")
		}
		commitHash := row[1].(string)
		commitMessage := row[2].(string)
		buffer.WriteString(fmt.Sprintf("%s %s %s\n", action, commitHash, commitMessage))
	}
	buffer.WriteString("\n")

	// help text
	rebaseBranchHash, err := getHashOf(queryist, sqlCtx, rebaseBranch)
	if err != nil {
		return "", err
	}
	currentBranchHash, err := getHashOf(queryist, sqlCtx, currentBranch)
	if err != nil {
		return "", err
	}
	rows, err = GetRowsForSql(queryist, sqlCtx, "SELECT COUNT(*) FROM dolt_rebase")
	if err != nil {
		return "", err
	}
	numSteps, err := getInt64ColAsInt64(rows[0][0])
	if err != nil {
		return "", err
	}
	buffer.WriteString(fmt.Sprintf("# Rebase %s..%s onto %s (%d commands)\n#\n", rebaseBranchHash, currentBranchHash, rebaseBranchHash, numSteps))

	buffer.WriteString("# Commands:\n")
	buffer.WriteString("# p, pick <commit> = use commit\n")
	buffer.WriteString("# d, drop <commit> = remove commit\n")
	buffer.WriteString("# r, reword <commit> = use commit, but edit the commit message\n")
	buffer.WriteString("# s, squash <commit> = use commit, but meld into previous commit\n")
	buffer.WriteString("# f, fixup <commit> = like \"squash\", but discard this commit's message\n")
	buffer.WriteString("# These lines can be re-ordered; they are executed from top to bottom.\n")
	buffer.WriteString("#\n")
	buffer.WriteString("# If you remove a line here THAT COMMIT WILL BE LOST.\n")
	buffer.WriteString("#\n")
	buffer.WriteString("# However, if you remove everything, the rebase will be aborted.\n")
	buffer.WriteString("#\n")

	return buffer.String(), nil
}

// getRebaseAction returns the rebase action for the given row. This conversion is necessary because a local client
// returns an int representing the enum whereas a remote client properly returns the label.
// TODO: Remove this once the local client returns the label.
func getRebaseAction(col interface{}) (string, bool) {
	action, ok := col.(string)
	if ok {
		return action, true
	} else {
		return dprocedures.RebaseActionEnumType.At(int(col.(uint16)))
	}
}

// parseRebaseMessage parses the rebase message from the editor and adds all uncommented out lines as steps in the rebase plan.
func parseRebaseMessage(rebaseMsg string) (*rebase.RebasePlan, error) {
	plan := &rebase.RebasePlan{}
	splitMsg := strings.Split(rebaseMsg, "\n")
	for i, line := range splitMsg {
		if !strings.HasPrefix(line, "#") && strings.ReplaceAll(line, " ", "") != "" {
			rebaseStepParts := strings.SplitN(line, " ", 3)
			if len(rebaseStepParts) != 3 {
				return nil, fmt.Errorf("invalid line %d: %s", i, line)
			}
			plan.Steps = append(plan.Steps, rebase.RebasePlanStep{
				Action:     rebaseStepParts[0],
				CommitHash: rebaseStepParts[1],
				CommitMsg:  rebaseStepParts[2],
			})
		}
	}

	return plan, nil
}

// insertRebasePlanIntoDoltRebaseTable inserts the rebase plan into the dolt_rebase table by re-building the dolt_rebase
// table from scratch.
func insertRebasePlanIntoDoltRebaseTable(plan *rebase.RebasePlan, sqlCtx *sql.Context, queryist cli.Queryist) error {
	_, err := GetRowsForSql(queryist, sqlCtx, "TRUNCATE TABLE dolt_rebase")
	if err != nil {
		return err
	}

	for i, step := range plan.Steps {
		_, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("INSERT INTO dolt_rebase VALUES (%d, '%s', '%s', '%s')", i+1, step.Action, step.CommitHash, step.CommitMsg))
		if err != nil {
			return err
		}
	}

	return nil
}
