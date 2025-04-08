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

package docscmds

import (
	"context"

	textdiff "github.com/andreyvit/diff"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var diffDocs = cli.CommandDocumentationContent{
	ShortDesc: "Diffs Dolt Docs",
	LongDesc:  "Diffs Dolt Docs",
	Synopsis: []string{
		"{{.LessThan}}doc{{.GreaterThan}}",
	},
}

type DiffCmd struct{}

// Name implements cli.Command.
func (cmd DiffCmd) Name() string {
	return "diff"
}

// Description implements cli.Command.
func (cmd DiffCmd) Description() string {
	return diffDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd DiffCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd DiffCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(diffDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd DiffCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"doc", "Dolt doc to be diffed."})
	return ap
}

// Exec implements cli.Command.
func (cmd DiffCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, diffDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 1 {
		verr := errhand.BuildDError("dolt docs diff takes exactly one argument").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	var verr errhand.VerboseError
	if err := diffDoltDoc(ctx, dEnv, apr.Arg(0)); err != nil {
		verr = errhand.VerboseErrorFromError(err)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func diffDoltDoc(ctx context.Context, dEnv *env.DoltEnv, docName string) error {
	eng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return err
	}
	sqlCtx, err := eng.NewLocalContext(ctx)
	if err != nil {
		return err
	}
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)
	sqlCtx.SetCurrentDatabase(dbName)

	working, err := readDocFromTable(sqlCtx, eng, docName)
	if err != nil {
		return err
	}

	head, err := readDocFromTableAsOf(sqlCtx, eng, docName, "HEAD")
	if err != nil {
		return err
	}

	cli.Print(textdiff.LineDiff(head, working))
	return nil
}
