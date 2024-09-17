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
		"influence of the specified commit. If multiple commits are specified, then this process is repeated for each " +
		"commit in the order specified. This requires a clean working set." +
		"\n\nAny conflicts or constraint violations caused by the merge cause the command to fail.",
	Synopsis: []string{
		"<revision>...",
	},
}

type RevertCmd struct{}

var _ cli.Command = RevertCmd{}

// Name implements the interface cli.Command.
func (cmd RevertCmd) Name() string {
	return "revert"
}

func (cmd RevertCmd) RequiresRepo() bool {
	return false
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

	// This command creates a commit, so we need user identity
	if !cli.CheckUserNameAndEmail(cliCtx.Config()) {
		return 1
	}

	if apr.NArg() < 1 {
		usage()
		return 1
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

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

	var params []interface{}
	params = append(params, author)

	var buffer bytes.Buffer
	buffer.WriteString("CALL DOLT_REVERT('--author', ?")
	// Loop over args and add them to the query
	for _, input := range apr.Args {
		buffer.WriteString(", ?")
		params = append(params, input)
	}
	buffer.WriteString(")")
	query, err := dbr.InterpolateForDialect(buffer.String(), params, dialect.MySQL)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, query)
	if err != nil {
		cli.Printf("Failure to execute '%s': %s\n", query, err.Error())
		return 1
	}
	_, err = sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}

	commit, err := getCommitInfo(queryist, sqlCtx, "HEAD")
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
