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
	"fmt"
	"io"
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

var uploadDocs = cli.CommandDocumentationContent{
	ShortDesc: "Uploads Dolt Docs from the file system into the database",
	LongDesc:  "Uploads Dolt Docs from the file system into the database",
	Synopsis: []string{
		"{{.LessThan}}doc{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
	},
}

type UploadCmd struct{}

// Name implements cli.Command.
func (cmd UploadCmd) Name() string {
	return "upload"
}

// Description implements cli.Command.
func (cmd UploadCmd) Description() string {
	return uploadDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd UploadCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd UploadCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(uploadDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd UploadCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"doc", "Dolt doc name to be updated in the database."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "file to read Dolt doc from."})
	return ap
}

// Exec implements cli.Command.
func (cmd UploadCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, uploadDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 2 {
		verr := errhand.BuildDError("dolt docs read takes exactly two arguments").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	if verr := validateDocName(apr.Arg(0)); verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	var verr errhand.VerboseError
	if err := readDoltDoc(ctx, dEnv, apr.Arg(0), apr.Arg(1)); err != nil {
		verr = errhand.VerboseErrorFromError(err)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func validateDocName(docName string) errhand.VerboseError {
	valid := []string{
		doltdb.ReadmeDoc,
		doltdb.LicenseDoc,
	}

	for _, name := range valid {
		if name == docName {
			return nil
		}
	}

	return errhand.BuildDError("invalid doc name '%s', valid names are (%s)",
		docName, strings.Join(valid, ", ")).Build()
}

func readDoltDoc(ctx context.Context, dEnv *env.DoltEnv, docName, fileName string) error {
	update, err := dEnv.FS.ReadFile(fileName)
	if err != nil {
		return err
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

	err = writeDocToTable(sqlCtx, eng, docName, string(update))
	if err != nil {
		return err
	}

	return nil
}

const (
	writeDocTemplate = `REPLACE INTO dolt_docs VALUES ("%s", "%s")`
)

func writeDocToTable(ctx *sql.Context, eng *engine.SqlEngine, docName, content string) error {
	var (
		err error
	)

	err = ctx.Session.SetSessionVariable(ctx, sql.AutoCommitSessionVar, 1)
	if err != nil {
		return err
	}
	ctx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})

	content = strings.ReplaceAll(content, `"`, `\"`)
	update := fmt.Sprintf(writeDocTemplate, docName, content)

	return execQuery(ctx, eng, update)
}

func execQuery(sctx *sql.Context, eng *engine.SqlEngine, q string) (err error) {
	var iter sql.RowIter
	_, iter, _, err = eng.Query(sctx, q)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := iter.Close(sctx); err == nil {
			err = cerr
		}
	}()

	for {
		_, err = iter.Next(sctx)
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return err
		}
	}
	return
}
