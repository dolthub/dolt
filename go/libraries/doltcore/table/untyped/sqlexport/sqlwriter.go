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
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
)

// SqlExportWriter is a TableWriter that writes SQL drop, create and insert statements to re-create a dolt table in a
// SQL database.
type SqlExportWriter struct {
	tableName       string
	sch             schema.Schema
	wr              io.WriteCloser
	writtenFirstRow bool
}

// OpenSQLExportWriter returns a new SqlWriter for the table given writing to a file with the path given.
func OpenSQLExportWriter(path string, tableName string, fs filesys.WritableFS, sch schema.Schema) (*SqlExportWriter, error) {
	err := fs.MkDirs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)
	if err != nil {
		return nil, err
	}

	return &SqlExportWriter{tableName: tableName, sch: sch, wr: wr}, nil
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
	if err := w.maybeWriteDropCreate(); err != nil {
		return err
	}

	stmt, err := sql.RowAsInsertStmt(r, w.tableName, w.sch)

	if err != nil {
		return err
	}

	return iohelp.WriteLine(w.wr, stmt)
}

func (w *SqlExportWriter) maybeWriteDropCreate() error {
	if !w.writtenFirstRow {
		var b strings.Builder
		b.WriteString(sql.DropTableIfExistsStmt(w.tableName))
		b.WriteRune('\n')
		b.WriteString(sql.SchemaAsCreateStmt(w.tableName, w.sch))
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
	if err := w.maybeWriteDropCreate(); err != nil {
		return err
	}

	if w.wr != nil {
		return w.wr.Close()
	}
	return nil
}

func (w *SqlExportWriter) WriteInsertRow(ctx context.Context, r row.Row) error {
	stmt, err := sql.RowAsInsertStmt(r, w.tableName, w.sch)

	if err != nil {
		return err
	}

	return iohelp.WriteLine(w.wr, stmt)
}

func (w *SqlExportWriter) WriteDeleteRow(ctx context.Context, r row.Row) error {
	stmt, err := sql.RowAsDeleteStmt(r, w.tableName, w.sch)

	if err != nil {
		return err
	}

	return iohelp.WriteLine(w.wr, stmt)
}

func (w *SqlExportWriter) WriteUpdateRow(ctx context.Context, r row.Row) error {
	stmt, err := sql.RowAsUpdateStmt(r, w.tableName, w.sch)

	if err != nil {
		return err
	}

	return iohelp.WriteLine(w.wr, stmt)
}
