// Copyright 2025 Dolthub, Inc.
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

package createchunk

import (
	"bytes"
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

// CreateCommitCmd creates a new commit chunk, printing the new chunk's hash on success.
// The user must supply a branch name, which will be set to this new commit.
// This is only required for the CLI command, and is optional when invoking the equivalent stored procedure.
// This is because the journal must end with a root hash, and is only flushed when there is a new root hash.
// Thus, we must update the root hash before the command finishes, or else changes will not be persisted.
type CreateCommitCmd struct{}

func generateCreateCommitSQL(cliCtx cli.CliContext, apr *argparser.ArgParseResults) (query string, params []interface{}, err error) {
	var buffer bytes.Buffer
	var first bool
	first = true
	buffer.WriteString("CALL DOLT_ADMIN_CREATECHUNK_COMMIT(")

	writeParam := func(key, val string) {
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString("'--")
		buffer.WriteString(key)
		buffer.WriteString("', ")
		buffer.WriteString("?")
		first = false
		params = append(params, val)
	}

	forwardParam := func(key string) {
		val, ok := apr.GetValue(key)
		if !ok {
			return
		}
		writeParam(key, val)
	}

	forwardFlag := func(flag string) {
		if !apr.Contains(flag) {
			return
		}
		if !first {
			buffer.WriteString(", ")
		}
		buffer.WriteString("'--")
		buffer.WriteString(flag)
		buffer.WriteString("'")
		first = false
	}

	var author string
	if apr.Contains(cli.AuthorParam) {
		author, _ = apr.GetValue(cli.AuthorParam)
	} else {
		name, email, err := env.GetNameAndEmail(cliCtx.Config())
		if err != nil {
			return "", nil, err
		}
		author = name + " <" + email + ">"
	}
	writeParam(cli.AuthorParam, author)

	forwardParam("desc")
	forwardParam("root")
	forwardParam("parents")
	forwardParam(cli.BranchParam)
	forwardFlag(cli.ForceFlag)

	buffer.WriteString(")")
	return buffer.String(), params, nil
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CreateCommitCmd) Name() string {
	return "commit"
}

// Description returns a description of the command
func (cmd CreateCommitCmd) Description() string {
	return "Creates a new commit chunk in the dolt storage"
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd CreateCommitCmd) RequiresRepo() bool {
	return false
}

func (cmd CreateCommitCmd) Docs() *cli.CommandDocumentation {
	// Admin commands are undocumented
	return nil
}

func (cmd CreateCommitCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCreateCommitParser()
}

func (cmd CreateCommitCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))

	// Ensure that the CLI args parse, but only check that a branch was supplied.
	// All other args will be validated in the system procedure, but the branch is only required in the CLI.
	apr := cli.ParseArgsOrDie(ap, args, usage)
	if !apr.Contains(cli.BranchParam) {
		cli.PrintErrf("the --%s flag is required when creating a chunk using the CLI", cli.BranchParam)
		return 1
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	querySql, params, err := generateCreateCommitSQL(cliCtx, apr)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}
	interpolatedQuery, err := dbr.InterpolateForDialect(querySql, params, dialect.MySQL)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, interpolatedQuery)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	rows, err := sql.RowIterToRows(sqlCtx, rowIter)
	if err != nil {
		cli.PrintErrln(errhand.VerboseErrorFromError(err))
		return 1
	}

	cli.Println(rows[0][0])

	return 0
}
