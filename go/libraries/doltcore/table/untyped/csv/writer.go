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

package csv

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"path/filepath"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// WriteBufSize is the size of the buffer used when writing a csv file.  It is set at the package level and all
// writers create their own buffer's using the value of this variable at the time they create their buffers.
var WriteBufSize = 256 * 1024

// CSVWriter implements TableWriter.  It writes rows as comma separated string values
type CSVWriter struct {
	closer io.Closer
	csvw   *csv.Writer
	info   *CSVFileInfo
	sch    schema.Schema
}

// OpenCSVWriter creates a file at the given path in the given filesystem and writes out rows based on the Schema,
// and CSVFileInfo provided
func OpenCSVWriter(path string, fs filesys.WritableFS, outSch schema.Schema, info *CSVFileInfo) (*CSVWriter, error) {
	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return nil, err
	}

	return NewCSVWriter(wr, outSch, info)
}

// NewCSVWriter writes rows to the given WriteCloser based on the Schema and CSVFileInfo provided
func NewCSVWriter(wr io.WriteCloser, outSch schema.Schema, info *CSVFileInfo) (*CSVWriter, error) {
	csvw := csv.NewWriter(wr)
	csvw.Comma = []rune(info.Delim)[0]

	if info.HasHeaderLine {
		allCols := outSch.GetAllCols()
		numCols := allCols.Size()
		colNames := make([]string, 0, numCols)
		err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			colNames = append(colNames, col.Name)
			return false, nil
		})

		if err != nil {
			wr.Close()
			return nil, err
		}

		err = csvw.Write(colNames)

		if err != nil {
			wr.Close()
			return nil, err
		}
	}

	return &CSVWriter{wr, csvw, info, outSch}, nil
}

// GetSchema gets the schema of the rows that this writer writes
func (csvw *CSVWriter) GetSchema() schema.Schema {
	return csvw.sch
}

// WriteRow will write a row to a table
func (csvw *CSVWriter) WriteRow(ctx context.Context, r row.Row) error {
	allCols := csvw.sch.GetAllCols()

	i := 0
	colValStrs := make([]string, allCols.Size())
	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val, ok := r.GetColVal(tag)
		if ok && !types.IsNull(val) {
			if val.Kind() == types.StringKind {
				colValStrs[i] = string(val.(types.String))
			} else {
				var err error
				colValStrs[i], err = types.EncodedValue(ctx, val)

				if err != nil {
					return false, err
				}
			}
		}

		i++
		return false, nil
	})

	if err != nil {
		return err
	}

	return csvw.csvw.Write(colValStrs)
}

// Close should flush all writes, release resources being held
func (csvw *CSVWriter) Close(ctx context.Context) error {
	if csvw.closer != nil {
		csvw.csvw.Flush()
		errCl := csvw.closer.Close()
		csvw.closer = nil
		return errCl
	} else {
		return errors.New("Already closed.")
	}
}
