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

package json

import (
	"context"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
)

type JsonDiffWriter struct {
	rowWriter   *RowWriter
	wr          io.WriteCloser
	rowsWritten int
	schemaDiffsWritten int
}

const jsonTableHeader = `{"table":{name: "%s","schema_diff":[`
const jsonTableFooter = `]}`

var _ diff.TableDiffWriter = (*JsonDiffWriter)(nil)

func NewJsonDiffWriter(wr io.WriteCloser, tableName string, outSch schema.Schema) (*JsonDiffWriter, error) {
	// leading diff type column with empty name
	cols := outSch.GetAllCols()
	newCols := schema.NewColCollection()
	newCols = newCols.Append(schema.NewColumn("diff_type", 0, types.StringKind, false))
	newCols = newCols.AppendColl(cols)

	newSchema, err := schema.SchemaFromCols(newCols)
	if err != nil {
		return nil, err
	}

	err = iohelp.WriteAll(wr, []byte(fmt.Sprintf(jsonTableHeader, tableName)))
	if err != nil {
		return nil, err
	}

	writer, err := NewJSONWriterWithHeader(iohelp.NopWrCloser(wr), newSchema, `"rows":[`, "]")
	if err != nil {
		return nil, err
	}

	return &JsonDiffWriter{
		rowWriter: writer,
		wr: wr,
	}, nil
}

func (j *JsonDiffWriter) WriteSchemaDiff(ctx context.Context, schemaDiffStatement string) error {
	// TODO implement me
	panic("implement me")
}

func (j *JsonDiffWriter) WriteRow(
		ctx context.Context,
		row sql.Row,
		rowDiffType diff.ChangeType,
		colDiffTypes []diff.ChangeType,
) error {
	if len(row) != len(colDiffTypes) {
		return fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
	}

	diffMarker := ""
	switch rowDiffType {
	case diff.Removed:
		diffMarker = "removed"
	case diff.Added:
		diffMarker = "added"
	case diff.ModifiedOld:
		diffMarker = "old_modified"
	case diff.ModifiedNew:
		diffMarker = "new_modified"
	}

	newRow := append(sql.Row{diffMarker}, row...)
	return j.rowWriter.WriteSqlRow(ctx, newRow)
}

func (j *JsonDiffWriter) Close(ctx context.Context) error {
	err := j.rowWriter.Close(ctx)
	if err != nil {
		return err
	}

	err = iohelp.WriteAll(j.wr, []byte(jsonTableFooter))
	if err != nil {
		return err
	}

	return j.wr.Close()
}
