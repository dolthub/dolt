// Copyright 2026 Dolthub, Inc.
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
	"bufio"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

// JSONLReader reads newline-delimited JSON objects, one per line.
type JSONLReader struct {
	vrw       types.ValueReadWriter
	closer    io.Closer
	sch       schema.Schema
	bRd       *bufio.Reader
	numLine   int
	sampleRow sql.Row
}

var _ table.SqlTableReader = (*JSONLReader)(nil)

func OpenJSONLReader(vrw types.ValueReadWriter, path string, fs filesys.ReadableFS, sch schema.Schema) (*JSONLReader, error) {
	r, err := fs.OpenForRead(path)
	if err != nil {
		return nil, err
	}

	return NewJSONLReader(vrw, r, sch)
}

// NewJSONLReader creates a JSONL reader. The bytes of the supplied reader are treated as UTF-8. If there is a UTF8,
// UTF16LE or UTF16BE BOM at the first bytes read, then it is stripped and the remaining contents of the reader are
// treated as that encoding.
func NewJSONLReader(vrw types.ValueReadWriter, r io.ReadCloser, sch schema.Schema) (*JSONLReader, error) {
	if sch == nil {
		return nil, errors.New("schema must be provided to JSONLReader")
	}

	textReader := transform.NewReader(r, unicode.BOMOverride(unicode.UTF8.NewDecoder()))
	br := bufio.NewReaderSize(textReader, ReadBufSize)

	return &JSONLReader{
		vrw:    vrw,
		closer: r,
		sch:    sch,
		bRd:    br,
	}, nil
}

func (r *JSONLReader) Close(ctx context.Context) error {
	if r.closer != nil {
		err := r.closer.Close()
		r.closer = nil
		return err
	}
	return nil
}

func (r *JSONLReader) GetSchema() schema.Schema {
	return r.sch
}

func (r *JSONLReader) VerifySchema(sch schema.Schema) (bool, error) {
	if r.sampleRow == nil {
		row, err := r.ReadSqlRow(context.Background())
		if err == nil {
			r.sampleRow = row
			return true, nil
		}
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *JSONLReader) ReadRow(ctx context.Context) (row.Row, error) {
	panic("deprecated")
}

func (r *JSONLReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	if r.sampleRow != nil {
		ret := r.sampleRow
		r.sampleRow = nil
		return ret, nil
	}

	for {
		line, done, err := iohelp.ReadLine(r.bRd)
		if err != nil {
			return nil, err
		}
		if done && line == "" {
			return nil, io.EOF
		}
		r.numLine++

		line = strings.TrimSpace(line)
		if line == "" {
			if done {
				return nil, io.EOF
			}
			continue
		}

		var val any
		if err := stdjson.Unmarshal([]byte(line), &val); err != nil {
			return nil, fmt.Errorf("invalid JSON at line %d: %w", r.numLine, err)
		}

		mapVal, ok := val.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected JSON object at line %d", r.numLine)
		}

		row, err := r.convToSqlRow(ctx, mapVal)
		if err != nil {
			return nil, fmt.Errorf("error converting JSONL row at line %d: %w", r.numLine, err)
		}

		return row, nil
	}
}

func (r *JSONLReader) convToSqlRow(ctx context.Context, rowMap map[string]interface{}) (sql.Row, error) {
	allCols := r.sch.GetAllCols()

	ret := make(sql.Row, allCols.Size())
	for k, v := range rowMap {
		col, ok := allCols.GetByName(k)
		if !ok {
			return nil, fmt.Errorf("column %s not found in schema", k)
		}

		v, _, err := col.TypeInfo.ToSqlType().Convert(ctx, v)
		if err != nil {
			return nil, err
		}

		idx := allCols.TagToIdx[col.Tag]
		ret[idx] = v
	}

	return ret, nil
}
