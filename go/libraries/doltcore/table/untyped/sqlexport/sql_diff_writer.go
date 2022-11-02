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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

type SqlDiffWriter struct {
	tableName            string
	sch                  schema.Schema
	writtenFirstRow      bool
	writtenAutocommitOff bool
	writeCloser          io.WriteCloser
	editOpts             editor.Options
	autocommitOff        bool
}

func NewSqlDiffWriter(tableName string, schema schema.Schema, wr io.WriteCloser) *SqlDiffWriter {
	return &SqlDiffWriter{
		tableName:       tableName,
		sch:             schema,
		writtenFirstRow: false,
		writeCloser:     wr,
	}
}

func (w SqlDiffWriter) WriteRow(
	ctx context.Context,
	row sql.Row,
	rowDiffType diff.ChangeType,
	colDiffTypes []diff.ChangeType,
) error {
	if len(row) != len(colDiffTypes) {
		return fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
	}

	switch rowDiffType {
	case diff.Added:
		stmt, err := sqlfmt.SqlRowAsInsertStmt(row, w.tableName, w.sch)
		if err != nil {
			return err
		}

		return iohelp.WriteLine(w.writeCloser, stmt)
	case diff.Removed:
		stmt, err := sqlfmt.SqlRowAsDeleteStmt(row, w.tableName, w.sch, 0)
		if err != nil {
			return err
		}

		return iohelp.WriteLine(w.writeCloser, stmt)
	case diff.ModifiedNew:
		updatedCols := set.NewEmptyStrSet()
		for i, diffType := range colDiffTypes {
			if diffType != diff.None {
				updatedCols.Add(w.sch.GetAllCols().GetByIndex(i).Name)
			}
		}

		stmt, err := sqlfmt.SqlRowAsUpdateStmt(row, w.tableName, w.sch, updatedCols)
		if err != nil {
			return err
		}

		return iohelp.WriteLine(w.writeCloser, stmt)
	case diff.ModifiedOld:
		// do nothing, we only issue UPDATE for ModifiedNew
		return nil
	default:
		return fmt.Errorf("unexpected row diff type: %v", rowDiffType)
	}
}

func (w SqlDiffWriter) Close(ctx context.Context) error {
	return w.writeCloser.Close()
}
