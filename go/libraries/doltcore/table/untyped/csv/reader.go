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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// ReadBufSize is the size of the buffer used when reading the csv file.  It is set at the package level and all
// readers create their own buffer's using the value of this variable at the time they create their buffers.
var ReadBufSize = 256 * 1024

// CSVReader implements TableReader.  It reads csv files and returns rows.
type CSVReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	info   *CSVFileInfo
	sch    schema.Schema
	isDone bool
	nbf    *types.NomsBinFormat
}

// OpenCSVReader opens a reader at a given path within a given filesys.  The CSVFileInfo should describe the csv file
// being opened.
func OpenCSVReader(nbf *types.NomsBinFormat, path string, fs filesys.ReadableFS, info *CSVFileInfo) (*CSVReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewCSVReader(nbf, r, info)
}

// NewCSVReader creates a CSVReader from a given ReadCloser.  The CSVFileInfo should describe the csv file being read.
func NewCSVReader(nbf *types.NomsBinFormat, r io.ReadCloser, info *CSVFileInfo) (*CSVReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)
	colStrs, err := getColHeaders(br, info)

	if err != nil {
		r.Close()
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	return &CSVReader{r, br, info, sch, false, nbf}, nil
}

func getColHeaders(br *bufio.Reader, info *CSVFileInfo) ([]string, error) {
	colStrs := info.Columns
	if info.HasHeaderLine {
		line, _, err := iohelp.ReadLine(br)

		if err != nil {
			return nil, err
		} else if strings.TrimSpace(line) == "" {
			return nil, errors.New("Header line is empty")
		}

		colStrsFromFile, err := csvSplitLine(line, info.Delim, info.EscapeQuotes)

		if err != nil {
			return nil, err
		}

		if colStrs == nil {
			colStrs = colStrsFromFile
		}
	}

	return colStrs, nil
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (csvr *CSVReader) ReadRow(ctx context.Context) (row.Row, error) {
	if csvr.isDone {
		return nil, io.EOF
	}

	var line string
	var err error
	isDone := false
	for line == "" && !isDone && err == nil {
		line, isDone, err = iohelp.ReadLine(csvr.bRd)

		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	csvr.isDone = isDone
	line = strings.TrimSpace(line)
	if line != "" {
		r, err := csvr.parseRow(line)
		return r, err
	} else if err == nil {
		return nil, io.EOF
	}

	return nil, err
}

// GetSchema gets the schema of the rows that this reader will return
func (csvr *CSVReader) GetSchema() schema.Schema {
	return csvr.sch
}

// Close should release resources being held
func (csvr *CSVReader) Close(ctx context.Context) error {
	if csvr.closer != nil {
		err := csvr.closer.Close()
		csvr.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}

func (csvr *CSVReader) parseRow(line string) (row.Row, error) {
	colVals, err := csvSplitLine(line, csvr.info.Delim, csvr.info.EscapeQuotes)

	if err != nil {
		return nil, table.NewBadRow(nil, err.Error())
	}

	sch := csvr.sch
	allCols := sch.GetAllCols()
	numCols := allCols.Size()
	if len(colVals) != numCols {
		return nil, table.NewBadRow(nil,
			fmt.Sprintf("csv reader's schema expects %d fields, but line only has %d values.", numCols, len(colVals)),
			fmt.Sprintf("line: '%s'", line),
		)
	}

	taggedVals := make(row.TaggedValues)
	for i := 0; i < allCols.Size(); i++ {
		if len(colVals[i]) > 0 {
			col := allCols.GetByIndex(i)
			taggedVals[col.Tag] = types.String(colVals[i])
		}
	}

	return row.New(csvr.nbf, sch, taggedVals)
}
