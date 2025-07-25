// Copyright 2023 Dolthub, Inc.
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

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/util/outputpager"
)

var reflogDocs = cli.CommandDocumentationContent{
	ShortDesc: "Shows a history of named refs",
	LongDesc: `Shows the history of named refs (e.g. branches and tags), which is useful for understanding how a branch 
or tag changed over time to reference different commits, particularly for information not surfaced through {{.EmphasisLeft}}dolt log{{.EmphasisRight}}.
The data from Dolt's reflog comes from [Dolt's journaling chunk store](https://www.dolthub.com/blog/2023-03-08-dolt-chunk-journal/). 
This data is local to a Dolt database and never included when pushing, pulling, or cloning a Dolt database. This means when you clone a Dolt database, it will not have any reflog data until you perform operations that change what commit branches or tags reference.

Dolt's reflog is similar to [Git's reflog](https://git-scm.com/docs/git-reflog), but there are a few differences:
- The Dolt reflog currently only supports named references, such as branches and tags, and not any of Git's special refs (e.g. {{.EmphasisLeft}}HEAD{{.EmphasisRight}}, {{.EmphasisLeft}}FETCH-HEAD{{.EmphasisRight}}, {{.EmphasisLeft}}MERGE-HEAD{{.EmphasisRight}}).
- The Dolt reflog can be queried for the log of references, even after a reference has been deleted. In Git, once a branch or tag is deleted, the reflog for that ref is also deleted and to find the last commit a branch or tag pointed to you have to use Git's special {{.EmphasisLeft}}HEAD{{.EmphasisRight}} reflog to find the commit, which can sometimes be challenging. Dolt makes this much easier by allowing you to see the history for a deleted ref so you can easily see the last commit a branch or tag pointed to before it was deleted.`,
	Synopsis: []string{
		`[--all] {{.LessThan}}ref{{.GreaterThan}}`,
	},
}

type ReflogCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ReflogCmd) Name() string {
	return "reflog"
}

// Description returns a description of the command
func (cmd ReflogCmd) Description() string {
	return "Show history of named refs."
}

// EventType returns the type of the event to log
func (cmd ReflogCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_REFLOG
}

func (cmd ReflogCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(reflogDocs, ap)
}

func (cmd ReflogCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateReflogArgParser()
}

func (cmd ReflogCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd ReflogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, reflogDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query, err := constructInterpolatedDoltReflogQuery(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	rows, err := GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return printReflog(rows, queryist, sqlCtx)
}

// constructInterpolatedDoltReflogQuery generates the sql query necessary to call the DOLT_REFLOG() function.
// Also interpolates this query to prevent sql injection
func constructInterpolatedDoltReflogQuery(apr *argparser.ArgParseResults) (string, error) {
	var params []interface{}
	var args []string

	if apr.NArg() == 1 {
		params = append(params, apr.Arg(0))
		args = append(args, "?")
	}
	if apr.Contains(cli.AllFlag) {
		args = append(args, "'--all'")
	}

	query := fmt.Sprintf("SELECT ref, commit_hash, commit_message FROM DOLT_REFLOG(%s)", strings.Join(args, ", "))
	interpolatedQuery, err := dbr.InterpolateForDialect(query, params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}

type ReflogInfo struct {
	ref           string
	commitHash    string
	commitMessage string
}

// printReflog takes a list of sql rows with columns ref, commit hash, commit message. Prints the reflog to stdout
func printReflog(rows []sql.Row, queryist cli.Queryist, sqlCtx *sql.Context) int {
	var reflogInfo []ReflogInfo

	// Get the hash of HEAD for the `HEAD ->` decoration
	headHash := ""
	res, err := GetRowsForSql(queryist, sqlCtx, "SELECT hashof('HEAD')")
	if err == nil {
		// still print the reflog even if we can't get the hash
		headHash = res[0][0].(string)
	}

	for _, row := range rows {
		ref := row[0].(string)
		commitHash := row[1].(string)
		commitMessage := row[2].(string)
		reflogInfo = append(reflogInfo, ReflogInfo{ref, commitHash, commitMessage})
	}

	reflogToStdOut(reflogInfo, headHash)

	return 0
}

// reflogToStdOut takes a list of ReflogInfo and prints the reflog to stdout
func reflogToStdOut(reflogInfo []ReflogInfo, headHash string) {
	if cli.ExecuteWithStdioRestored == nil {
		return
	}
	cli.ExecuteWithStdioRestored(func() {
		pager := outputpager.Start()
		defer pager.Stop()

		for _, info := range reflogInfo {
			// TODO: use short hash instead
			line := []string{fmt.Sprintf("\033[33m%s\033[0m", info.commitHash)} // commit hash in yellow (33m)

			processedRef := processRefForReflog(info.ref)
			if headHash != "" && headHash == info.commitHash {
				line = append(line, fmt.Sprintf("\033[33m(\033[36;1mHEAD -> %s\033[33m)\033[0m", processedRef)) // HEAD in cyan (36;1)
			} else {
				line = append(line, fmt.Sprintf("\033[33m(%s\033[33m)\033[0m", processedRef)) // () in yellow (33m)
			}
			line = append(line, fmt.Sprintf("%s\n", info.commitMessage))
			pager.Writer.Write([]byte(strings.Join(line, " ")))
		}
	})
}

// processRefForReflog takes a full ref (e.g. refs/heads/master) or tag name and returns the ref name (e.g. master) with relevant decoration.
func processRefForReflog(fullRef string) string {
	if strings.HasPrefix(fullRef, "refs/heads/") {
		return fmt.Sprintf("\033[32;1m%s\033[0m", strings.TrimPrefix(fullRef, "refs/heads/")) // branch in green (32;1m)
	} else if strings.HasPrefix(fullRef, "refs/tags/") {
		return fmt.Sprintf("\033[33mtag: %s\033[0m", strings.TrimPrefix(fullRef, "refs/tags/")) // tag in yellow (33m)
	} else if strings.HasPrefix(fullRef, "refs/remotes/") {
		return fmt.Sprintf("\033[31;1m%s\033[0m", strings.TrimPrefix(fullRef, "refs/remotes/")) // remote in red (31;1m)
	} else if strings.HasPrefix(fullRef, "refs/workspaces/") {
		return fmt.Sprintf("\033[35;1mworkspace: %s\033[0m", strings.TrimPrefix(fullRef, "refs/workspaces/")) // workspace in magenta (35;1m)
	} else {
		return fullRef
	}
}
