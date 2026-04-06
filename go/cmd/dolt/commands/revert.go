// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

var revertDocs = cli.CommandDocumentationContent{
	ShortDesc: "Undo the changes introduced in a commit",
	LongDesc: "Removes the changes made in a commit (or series of commits) from the working set, and then automatically " +
		"commits the result. This is done by way of a three-way merge. Given a specific commit " +
		"(e.g. {{.EmphasisLeft}}HEAD~1{{.EmphasisRight}}), this is similar to applying the patch from " +
		"{{.EmphasisLeft}}HEAD~1..HEAD~2{{.EmphasisRight}}, giving us a patch of what to remove to effectively remove the " +
		"influence of the specified commit. If multiple commits are specified, then each is reverted in the order given, " +
		"creating a separate commit for each revert. This requires a clean working set." +
		"\n\nIf conflicts or constraint violations are encountered during a revert, the operation pauses and leaves the " +
		"conflicting state in the working set. Resolve the conflicts, stage the resolved tables with " +
		"{{.EmphasisLeft}}dolt add{{.EmphasisRight}}, and then run {{.EmphasisLeft}}dolt revert --continue{{.EmphasisRight}} " +
		"to complete the revert. To abandon the revert entirely, run {{.EmphasisLeft}}dolt revert --abort{{.EmphasisRight}}.",
	Synopsis: []string{
		"<revision>...",
		"--continue",
		"--abort",
	},
}

type RevertCmd struct{}

var _ cli.Command = RevertCmd{}

// Name implements the interface cli.Command.
func (cmd RevertCmd) Name() string {
	return "revert"
}

// Description implements the interface cli.Command.
func (cmd RevertCmd) Description() string {
	return "Undo the changes introduced in a commit."
}

func (cmd RevertCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateRevertArgParser()
	return cli.NewCommandDocumentation(revertDocs, ap)
}

func (cmd RevertCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateRevertArgParser()
}

// Exec implements the interface cli.Command.
func (cmd RevertCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateRevertArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, revertDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.Contains(cli.AbortParam) && apr.Contains(cli.ContinueFlag) {
		cli.Println("error: --continue and --abort are mutually exclusive")
		return 1
	}

	// This command creates a commit, so we need user identity.
	if !cli.CheckUserNameAndEmail(cliCtx.Config()) {
		return 1
	}

	if apr.NArg() == 0 && !(apr.Contains(cli.ContinueFlag) || apr.Contains(cli.AbortParam)) {
		usage()
		return 1
	}

	return revert(ctx, apr, cliCtx)
}

func revert(ctx context.Context, apr *argparser.ArgParseResults, cliCtx cli.CliContext) int {
	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	var params []interface{}
	var buffer bytes.Buffer
	if apr.Contains(cli.AbortParam) {
		buffer.WriteString("CALL DOLT_REVERT('--abort')")
	} else if apr.Contains(cli.ContinueFlag) {
		var author string
		if apr.Contains(cli.AuthorParam) {
			author, _ = apr.GetValue(cli.AuthorParam)
		} else {
			name, email, err := env.GetNameAndEmail(cliCtx.Config())
			if err != nil {
				cli.Println(err.Error())
				return 1
			}
			author = fmt.Sprintf("%s <%s>", name, email)
		}
		params = append(params, author)
		buffer.WriteString("CALL DOLT_REVERT('--author', ?, '--continue')")
	} else {
		var author string
		if apr.Contains(cli.AuthorParam) {
			author, _ = apr.GetValue(cli.AuthorParam)
		} else {
			name, email, err := env.GetNameAndEmail(cliCtx.Config())
			if err != nil {
				cli.Println(err.Error())
				return 1
			}
			author = fmt.Sprintf("%s <%s>", name, email)
		}
		params = append(params, author)
		buffer.WriteString("CALL DOLT_REVERT('--author', ?")
		for _, input := range apr.Args {
			buffer.WriteString(", ?")
			params = append(params, input)
		}
		buffer.WriteString(")")
	}

	_, err = cli.GetRowsForSql(queryist.Queryist, queryist.Context, "set @@dolt_allow_commit_conflicts = 1")
	if err != nil {
		cli.Println(fmt.Errorf("error: failed to set @@dolt_allow_commit_conflicts: %w", err))
		return 1
	}

	_, err = cli.GetRowsForSql(queryist.Queryist, queryist.Context, "set @@dolt_force_transaction_commit = 1")
	if err != nil {
		cli.Println(fmt.Errorf("error: failed to set @@dolt_force_transaction_commit: %w", err))
		return 1
	}

	query, err := dbr.InterpolateForDialect(buffer.String(), params, dialect.MySQL)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	_, rowIter, _, err := queryist.Queryist.Query(queryist.Context, query)
	if err != nil {
		cli.Printf("error: %s\n", err.Error())
		return 1
	}

	rows, err := sql.RowIterToRows(queryist.Context, rowIter)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	if apr.Contains(cli.AbortParam) {
		cli.Println("Revert aborted.")
		return 0
	}

	// Check conflict counts from the result row.
	if len(rows) > 0 {
		row := rows[0]
		dataConflicts, err := cli.QueryValueAsInt64(row[1])
		if err != nil {
			cli.Println(err.Error())
			return 1
		}

		schemaConflicts, err := cli.QueryValueAsInt64(row[2])
		if err != nil {
			cli.Println(err.Error())
			return 1
		}

		constraintViolations, err := cli.QueryValueAsInt64(row[3])
		if err != nil {
			cli.Println(err.Error())
			return 1
		}

		if dataConflicts > 0 || schemaConflicts > 0 || constraintViolations > 0 {
			cli.Println("Automatic revert failed; fix conflicts and then commit the result.")
			if dataConflicts > 0 {
				cli.Printf("  %d table(s) have data conflicts\n", dataConflicts)
			}
			if schemaConflicts > 0 {
				cli.Printf("  %d table(s) have schema conflicts\n", schemaConflicts)
			}
			if constraintViolations > 0 {
				cli.Printf("  %d table(s) have constraint violations\n", constraintViolations)
			}
			cli.Println(`hint: After resolving conflicts, mark them with "dolt add <table>"`)
			cli.Println(`hint: and run "dolt revert --continue". To abort, run "dolt revert --abort".`)
			return 1
		}
	}

	commit, err := getCommitInfo(queryist.Context, queryist.Queryist, "HEAD")
	if err != nil {
		cli.Printf("Revert completed, but failure to get commit details occurred: %s\n", err.Error())
		return 1
	}
	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		PrintCommitInfo(pager, 0, false, false, "auto", commit)
	})

	return 0
}
