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
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var printDocs = cli.CommandDocumentationContent{
	ShortDesc: "Prints Dolt Docs to stdout",
	LongDesc:  "Prints Dolt Docs to stdout",
	Synopsis: []string{
		"{{.LessThan}}doc{{.GreaterThan}}",
	},
}

type PrintCmd struct{}

// Name implements cli.Command.
func (cmd PrintCmd) Name() string {
	return "print"
}

// Description implements cli.Command.
func (cmd PrintCmd) Description() string {
	return printDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd PrintCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd PrintCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(printDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd PrintCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"doc", "Dolt doc to be read."})
	return ap
}

// Exec implements cli.Command.
func (cmd PrintCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, printDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 1 {
		verr := errhand.BuildDError("dolt docs write takes exactly one argument").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	var verr errhand.VerboseError
	if err := writeDoltDoc(ctx, dEnv, apr.Arg(0)); err != nil {
		verr = errhand.VerboseErrorFromError(err)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func writeDoltDoc(ctx context.Context, dEnv *env.DoltEnv, docName string) error {
	// if we can't upload a doc with a different name, we shouldn't be able to try to read it either
	valid := []string{doltdb.ReadmeDoc, doltdb.LicenseDoc, doltdb.AgentDoc}
	if !slices.Contains(valid, docName) {
		return fmt.Errorf("invalid doc name %s, valid names are: %s", docName, strings.Join(valid, ", "))
	}

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

	doc, err := readDocFromTable(sqlCtx, eng, docName)
	if err != nil {
		return err
	}

	cli.Print(doc)
	return nil
}

const (
	readDocTemplate = "SELECT " + doltdb.DocTextColumnName + " " +
		"FROM dolt_docs %s WHERE " + doltdb.DocPkColumnName + " = '%s'"
)

func readDocFromTable(ctx *sql.Context, eng *engine.SqlEngine, docName string) (string, error) {
	return readDocFromTableAsOf(ctx, eng, docName, "")
}

func readDocFromTableAsOf(ctx *sql.Context, eng *engine.SqlEngine, docName, asOf string) (doc string, err error) {
	var (
		iter sql.RowIter
		row  sql.Row
	)

	if asOf != "" {
		asOf = fmt.Sprintf("AS OF '%s'", asOf)
	}
	query := fmt.Sprintf(readDocTemplate, asOf, docName)

	_, iter, _, err = eng.Query(ctx, query)
	if sql.ErrTableNotFound.Is(err) {
		return "", errors.New("no dolt docs in this database")
	}
	if err != nil {
		return "", err
	}

	defer func() {
		if cerr := iter.Close(ctx); err == nil {
			err = cerr
		}
	}()

	row, err = iter.Next(ctx)
	if err == io.EOF {
		// doc does not exist
		return "", nil
	}
	if err != nil {
		return "", err
	}

	var ok bool
	doc, ok, err = sql.Unwrap[string](ctx, row[0])
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("unexpected type for %s: expected string, found %T", doltdb.DocTextColumnName, row[0])
	}

	_, eof := iter.Next(ctx)
	if eof != io.EOF && eof != nil {
		return "", eof
	}
	return
}

func assertTrue(b bool) {
	if !b {
		panic("expected true")
	}
}
