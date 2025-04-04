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

package sqlexport

import (
	"context"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

const batchSize = 10000

// SqlExportWriter is a TableWriter that writes SQL drop, create and insert statements to re-create a dolt table in a
// SQL database.
type BatchSqlExportWriter struct {
	tableName            string
	sch                  schema.Schema
	parentSchs           map[doltdb.TableName]schema.Schema
	foreignKeys          []doltdb.ForeignKey
	wr                   io.WriteCloser
	root                 doltdb.RootValue
	writtenFirstRow      bool
	writtenAutocommitOff bool
	numInserts           int
	editOpts             editor.Options
	autocommitOff        bool
}

// OpenBatchedSQLExportWriter returns a new SqlWriter for the table with the writer given.
func OpenBatchedSQLExportWriter(
	ctx context.Context,
	wr io.WriteCloser,
	root doltdb.RootValue,
	tableName string,
	autocommitOff bool,
	sch schema.Schema,
	editOpts editor.Options,
) (*BatchSqlExportWriter, error) {

	allSchemas, err := doltdb.GetAllSchemas(ctx, root)
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, errhand.BuildDError("error: failed to read foreign key struct").AddCause(err).Build()
	}

	// TODO: schema name
	foreignKeys, _ := fkc.KeysForTable(doltdb.TableName{Name: tableName})

	return &BatchSqlExportWriter{
		tableName:     tableName,
		sch:           sch,
		parentSchs:    allSchemas,
		foreignKeys:   foreignKeys,
		root:          root,
		wr:            wr,
		editOpts:      editOpts,
		autocommitOff: autocommitOff,
	}, nil
}

// GetSchema returns the schema of this TableWriter.
func (w *BatchSqlExportWriter) GetSchema() schema.Schema {
	return w.sch
}

func (w *BatchSqlExportWriter) WriteSqlRow(ctx *sql.Context, r sql.Row) error {
	if err := w.maybeWriteDropCreate(ctx); err != nil {
		return err
	}

	if err := w.maybeWriteAutocommitOff(); err != nil {
		return err
	}

	// Reached max number of inserts on one line
	if w.numInserts == batchSize {
		// Reset count
		w.numInserts = 0

		// End line
		err := iohelp.WriteLine(w.wr, ";")
		if err != nil {
			return err
		}
	}

	// Append insert values as tuples
	var stmt string
	if w.numInserts == 0 {
		// Get insert prefix string
		prefix, err := sqlfmt.InsertStatementPrefix(w.tableName, w.sch)
		if err != nil {
			return nil
		}
		// Write prefix
		err = iohelp.WriteWithoutNewLine(w.wr, prefix)
		if err != nil {
			return nil
		}
	} else {
		stmt = ", "
	}

	// Get insert tuple string
	tuple, err := sqlfmt.SqlRowAsTupleString(ctx, r, w.sch)
	if err != nil {
		return err
	}

	// Write insert tuple
	err = iohelp.WriteWithoutNewLine(w.wr, stmt+tuple)
	if err != nil {
		return nil
	}

	// Increase count of inserts written on this line
	w.numInserts++

	return err
}

func (w *BatchSqlExportWriter) maybeWriteDropCreate(ctx context.Context) error {
	if w.writtenFirstRow {
		return nil
	}

	var b strings.Builder
	b.WriteString(sqlfmt.DropTableIfExistsStmt(w.tableName))
	b.WriteRune('\n')
	sqlCtx, engine, _ := dsqle.PrepareCreateTableStmt(ctx, dsqle.NewUserSpaceDatabase(w.root, w.editOpts))
	createTableStmt, err := dsqle.GetCreateTableStmt(sqlCtx, engine, w.tableName)
	if err != nil {
		return err
	}
	b.WriteString(createTableStmt)
	if err := iohelp.WriteLine(w.wr, b.String()); err != nil {
		return err
	}

	w.writtenFirstRow = true

	return nil
}

func (w *BatchSqlExportWriter) maybeWriteAutocommitOff() error {
	if w.writtenAutocommitOff || !w.autocommitOff {
		return nil
	}

	var b strings.Builder
	b.WriteString("SET AUTOCOMMIT=0;")

	if err := iohelp.WriteLine(w.wr, b.String()); err != nil {
		return err
	}

	w.writtenAutocommitOff = true

	return nil
}

// Close should flush all writes, release resources being held
func (w *BatchSqlExportWriter) Close(ctx context.Context) error {
	// exporting an empty table will not get any WriteRow calls, so write the drop / create here
	if err := w.maybeWriteDropCreate(ctx); err != nil {
		return err
	}

	// if wrote at least 1 insert, write the semicolon
	if w.numInserts > 0 {
		err := iohelp.WriteLine(w.wr, ";")
		if err != nil {
			return err
		}
	}

	// We have to commit the changes of this tables insert by adding a COMMIT statement.
	if w.autocommitOff {
		var b strings.Builder
		b.WriteString("COMMIT;")
		b.WriteRune('\n')
		if err := iohelp.WriteLine(w.wr, b.String()); err != nil {
			return err
		}
	}

	if w.wr != nil {
		return w.wr.Close()
	}
	return nil
}
