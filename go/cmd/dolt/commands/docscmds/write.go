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
	"github.com/kioopi/extedit"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var writeDocs = cli.CommandDocumentationContent{
	ShortDesc: "",
	LongDesc:  ``,
	Synopsis:  []string{},
}

type WriteCmd struct{}

// Name implements cli.Command.
func (cmd WriteCmd) Name() string {
	return "write"
}

// Description implements cli.Command.
func (cmd WriteCmd) Description() string {
	return writeDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd WriteCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd WriteCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(diffDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd WriteCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec implements cli.Command.
func (cmd WriteCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	panic("dolt docs write")
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

	// open an editor to edit the file
	diff, err := extedit.Invoke(strings.NewReader(doc))
	if err != nil {
		return err
	}

	return writeDocToTable(ctx, eng, docName, diff.Content.String())
}

const (
	writeDocTemplate = "INSERT INTO dolt_docs VALUES (%s, %s)"
)

func writeDocToTable(ctx context.Context, eng *engine.SqlEngine, docName, content string) (err error) {
	fmt.Printf("write doc %s with new contents %s", docName, content)

	var (
		sctx *sql.Context
		iter sql.RowIter
		row  sql.Row
	)

	sctx, err = eng.NewContext(ctx)
	if err != nil {
		return err
	}
	sctx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})

	_, iter, err = eng.Query(sctx, fmt.Sprintf(writeDocTemplate, docName, content))
	if err != nil {
		return err
	}
	defer func() {
		if cerr := iter.Close(sctx); err == nil {
			err = cerr
		}
	}()

	row, err = iter.Next(sctx)
	assertTrue(err == io.EOF)
	assertTrue(row == nil)
	return
}

func maybeCreateDoltDocs(ctx context.Context, dEnv *env.DoltEnv) error {
	return nil
}
