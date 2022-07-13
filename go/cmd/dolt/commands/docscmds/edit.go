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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var writeDocs = cli.CommandDocumentationContent{
	ShortDesc: "Edits Dolt docs using the default editor",
	LongDesc:  ``,
	Synopsis:  []string{},
}

type EditCmd struct{}

// Name implements cli.Command.
func (cmd EditCmd) Name() string {
	return "edit"
}

// Description implements cli.Command.
func (cmd EditCmd) Description() string {
	return writeDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd EditCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd EditCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(writeDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd EditCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"doc", "Dolt doc to be edited."})
	return ap
}

// Exec implements cli.Command.
func (cmd EditCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, readDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 1 {
		verr := errhand.BuildDError("dolt docs edit takes exactly one argument").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	var verr errhand.VerboseError
	if err := editDoltDoc(ctx, dEnv, apr.Arg(0)); err != nil {
		verr = errhand.VerboseErrorFromError(err)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func editDoltDoc(ctx context.Context, dEnv *env.DoltEnv, docName string) error {
	if err := maybeCreateDoltDocs(ctx, dEnv); err != nil {
		return err
	}

	eng, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	if err != nil {
		return err
	}

	doc, err := readDocFromTable(ctx, eng, docName)
	if err != nil {
		return err
	}
	fmt.Printf("old doc: %s", doc)

	// open an editor to edit the file
	//diff, err := extedit.Invoke(strings.NewReader(doc))
	//if err != nil {
	//	return err
	//}
	update := "this is a new doc"

	root, err := writeDocToTable(ctx, eng, docName, update)
	if err != nil {
		return err
	}

	return dEnv.UpdateWorkingRoot(ctx, root)
}

const (
	writeDocTemplate = "REPLACE INTO dolt_docs VALUES ('%s', '%s')"
)

func writeDocToTable(ctx context.Context, eng *engine.SqlEngine, docName, content string) (*doltdb.RootValue, error) {
	fmt.Printf("write doc %s with new contents %s", docName, content)

	var (
		sctx  *sql.Context
		iter  sql.RowIter
		err   error
		roots map[string]*doltdb.RootValue
	)

	sctx, err = eng.NewContext(ctx)
	if err != nil {
		return nil, err
	}
	sctx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})

	_, iter, err = eng.Query(sctx, fmt.Sprintf(writeDocTemplate, docName, content))
	if err != nil {
		return nil, err
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
			return nil, err
		}
	}

	if roots, err = eng.GetRoots(sctx); err != nil {
		return nil, err
	}
	assertTrue(len(roots) == 1)

	for _, rv := range roots {
		return rv, nil
	}
	panic("unreachable")
}

func maybeCreateDoltDocs(ctx context.Context, dEnv *env.DoltEnv) error {
	return nil
}
