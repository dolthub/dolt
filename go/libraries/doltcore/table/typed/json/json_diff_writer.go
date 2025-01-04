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
	"encoding/json"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

type jsonRowDiffWriter struct {
	rowWriter   *RowWriter
	wr          io.WriteCloser
	inModified  bool
	rowsWritten int
}

var _ diff.SqlRowDiffWriter = (*jsonRowDiffWriter)(nil)

func NewJSONRowDiffWriter(wr io.WriteCloser, outSch schema.Schema) (*jsonRowDiffWriter, error) {
	writer, err := NewJSONWriterWithHeader(iohelp.NopWrCloser(wr), outSch, "", "", "")
	if err != nil {
		return nil, err
	}

	return &jsonRowDiffWriter{
		rowWriter: writer,
		wr:        wr,
	}, nil
}

func (j *jsonRowDiffWriter) WriteRow(
	ctx context.Context,
	row sql.Row,
	rowDiffType diff.ChangeType,
	colDiffTypes []diff.ChangeType,
) error {
	if row.Len() != len(colDiffTypes) {
		return fmt.Errorf("expected the same size for columns and diff types, got %d and %d", row.Len(), len(colDiffTypes))
	}

	prefix := ""
	if j.inModified {
		prefix = ","
	} else if j.rowsWritten > 0 {
		prefix = ",{"
	} else {
		prefix = "{"
	}

	err := iohelp.WriteAll(j.wr, []byte(prefix))
	if err != nil {
		return err
	}

	diffMarker := ""
	switch rowDiffType {
	case diff.Removed:
		diffMarker = "from_row"
	case diff.ModifiedOld:
		diffMarker = "from_row"
	case diff.Added:
		err := iohelp.WriteAll(j.wr, []byte(fmt.Sprintf(`"%s":{},`, "from_row")))
		if err != nil {
			return err
		}
		diffMarker = "to_row"
	case diff.ModifiedNew:
		diffMarker = "to_row"
	}

	err = iohelp.WriteAll(j.wr, []byte(fmt.Sprintf(`"%s":`, diffMarker)))
	if err != nil {
		return err
	}

	err = j.rowWriter.WriteSqlRow(ctx, row)
	if err != nil {
		return err
	}

	// The row writer buffers its output and we share an underlying write stream with it, so we need to flush after
	// every call to WriteSqlRow
	err = j.rowWriter.Flush()
	if err != nil {
		return err
	}

	switch rowDiffType {
	case diff.ModifiedNew, diff.ModifiedOld:
		j.inModified = !j.inModified
	case diff.Added:
	case diff.Removed:
		err := iohelp.WriteAll(j.wr, []byte(fmt.Sprintf(`,"%s":{}`, "to_row")))
		if err != nil {
			return err
		}
	}

	if !j.inModified {
		err := iohelp.WriteAll(j.wr, []byte("}"))
		if err != nil {
			return err
		}
		j.rowsWritten++
	}

	return nil
}

func (j *jsonRowDiffWriter) WriteCombinedRow(ctx context.Context, oldRow, newRow sql.Row, mode diff.Mode) error {
	return fmt.Errorf("json format is unable to output diffs for combined rows")
}

func (j *jsonRowDiffWriter) Close(ctx context.Context) error {
	err := iohelp.WriteAll(j.wr, []byte("]"))
	if err != nil {
		return err
	}

	err = j.rowWriter.Close(ctx)
	if err != nil {
		return err
	}

	return j.wr.Close()
}

type SchemaDiffWriter struct {
	wr                 io.WriteCloser
	schemaStmtsWritten int
}

var _ diff.SchemaDiffWriter = (*SchemaDiffWriter)(nil)

const jsonSchemaHeader = `"schema_diff":[`
const jsonSchemaFooter = `],`
const jsonSchemaSep = `,`

func NewSchemaDiffWriter(wr io.WriteCloser) (*SchemaDiffWriter, error) {
	err := iohelp.WriteAll(wr, []byte(jsonSchemaHeader))
	if err != nil {
		return nil, err
	}

	return &SchemaDiffWriter{
		wr: wr,
	}, nil
}

func (j *SchemaDiffWriter) WriteSchemaDiff(schemaDiffStatement string) error {
	if j.schemaStmtsWritten > 0 {
		err := iohelp.WriteAll(j.wr, []byte(jsonSchemaSep))
		if err != nil {
			return err
		}
	}

	jsonSchemaDiffStmt := fmt.Sprintf("%s", jsonEscape(schemaDiffStatement))
	err := iohelp.WriteAll(j.wr, []byte(jsonSchemaDiffStmt))
	if err != nil {
		return err
	}

	j.schemaStmtsWritten++

	return nil
}

func (j *SchemaDiffWriter) Close() error {
	err := iohelp.WriteAll(j.wr, []byte(jsonSchemaFooter))
	if err != nil {
		return err
	}

	err = j.wr.Close()
	if err != nil {
		return err
	}

	return nil
}

func jsonEscape(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
