// Copyright 2019 Liquidata, Inc.
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

package sqlexport

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

// SqlExportWriter is a TableWriter that writes SQL drop, create and insert statements to re-create a dolt table in a
// SQL database.
type SqlExportWriter struct {
	tableName       string
	sch             schema.Schema
	parentSchs      map[string]schema.Schema
	foreignKeys     []doltdb.ForeignKey
	wr              io.WriteCloser
	root            *doltdb.RootValue
	writtenFirstRow bool
}

// OpenSQLExportWriter returns a new SqlWriter for the table given writing to a file with the path given.
func OpenSQLExportWriter(ctx context.Context, path string, fs filesys.WritableFS, root *doltdb.RootValue, tableName string, sch schema.Schema) (*SqlExportWriter, error) {
	err := fs.MkDirs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	allSchemas, err := root.GetAllSchemas(ctx)
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to read foreign key struct").AddCause(err).Build()
	}

	foreignKeys, _ := fkc.KeysForTable(tableName)

	return &SqlExportWriter{
		tableName:   tableName,
		sch:         sch,
		parentSchs:  allSchemas,
		foreignKeys: foreignKeys,
		root:        root,
		wr:          wr,
	}, nil
}

func NewSQLDiffWriter(wr io.WriteCloser, tableName string, sch schema.Schema) (*SqlExportWriter, error) {
	// set writtenFirstRow = true to prevent table drop statement from being written
	return &SqlExportWriter{tableName: tableName, sch: sch, wr: wr, writtenFirstRow: true}, nil
}

// Returns the schema of this TableWriter.
func (w *SqlExportWriter) GetSchema() schema.Schema {
	return w.sch
}

// WriteRow will write a row to a table
func (w *SqlExportWriter) WriteRow(ctx context.Context, r row.Row) error {
	if err := w.maybeWriteDropCreate(ctx); err != nil {
		return err
	}

	stmt, err := sqlfmt.RowAsInsertStmt(r, w.tableName, w.sch)

	if err != nil {
		return err
	}

	return iohelp.WriteLine(w.wr, stmt)
}

func (w *SqlExportWriter) maybeWriteDropCreate(ctx context.Context) error {
	if !w.writtenFirstRow {
		var b strings.Builder
		b.WriteString(sqlfmt.DropTableIfExistsStmt(w.tableName))
		b.WriteRune('\n')
		sqlCtx, engine, _ := dsqle.PrepareCreateTableStmt(ctx, w.root)
		createTableStmt, err := dsqle.GetCreateTableStmt(sqlCtx, engine, w.tableName)
		if err != nil {
			return err
		}
		b.WriteString(createTableStmt)
		if err := iohelp.WriteLine(w.wr, b.String()); err != nil {
			return err
		}
		w.writtenFirstRow = true
	}
	return nil
}

// Close should flush all writes, release resources being held
func (w *SqlExportWriter) Close(ctx context.Context) error {
	// exporting an empty table will not get any WriteRow calls, so write the drop / create here
	if err := w.maybeWriteDropCreate(ctx); err != nil {
		return err
	}

	if w.wr != nil {
		return w.wr.Close()
	}
	return nil
}
