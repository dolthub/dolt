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

package fwt

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

// ReadBufSize is the size of the buffer used when reading the fwt file.  It is set at the package level and all
// readers create their own buffer's using the value of this variable at the time they create their buffers.
var ReadBufSize = 256 * 1024

// FWTReader implements TableReader.  It reads fwt files and returns rows.
type FWTReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	fwtSch *FWTSchema
	isDone bool
	colSep string
	nbf    *types.NomsBinFormat
}

// OpenFWTReader opens a reader at a given path within a given filesys.  The FWTSchema should describe the fwt file
// being opened and have the correct column widths.
func OpenFWTReader(nbf *types.NomsBinFormat, path string, fs filesys.ReadableFS, fwtSch *FWTSchema, colSep string) (*FWTReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewFWTReader(nbf, r, fwtSch, colSep)
}

func NewFWTReader(nbf *types.NomsBinFormat, r io.ReadCloser, fwtSch *FWTSchema, colSep string) (*FWTReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)

	return &FWTReader{r, br, fwtSch, false, colSep, nbf}, nil
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (fwtRd *FWTReader) ReadRow(ctx context.Context) (row.Row, error) {
	if fwtRd.isDone {
		return nil, io.EOF
	}

	var line string
	var err error
	isDone := false
	for line == "" && !isDone && err == nil {
		line, isDone, err = iohelp.ReadLine(fwtRd.bRd)

		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	fwtRd.isDone = isDone
	if line != "" {
		r, err := fwtRd.parseRow([]byte(line))
		return r, err
	} else if err == nil {
		return nil, io.EOF
	}

	return nil, err
}

// GetSchema gets the schema of the rows that this reader will return
func (fwtRd *FWTReader) GetSchema() schema.Schema {
	return fwtRd.fwtSch.Sch
}

// Close should release resources being held
func (fwtRd *FWTReader) Close(ctx context.Context) error {
	if fwtRd.closer != nil {
		err := fwtRd.closer.Close()
		fwtRd.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}

func (fwtRd *FWTReader) parseRow(lineBytes []byte) (row.Row, error) {
	sepWidth := len(fwtRd.colSep)
	expectedBytes := fwtRd.fwtSch.GetTotalWidth(sepWidth)
	if len(lineBytes) != expectedBytes {
		return nil, table.NewBadRow(nil, fmt.Sprintf("expected a line containing %d bytes, but only received %d", len(lineBytes), expectedBytes))
	}

	allCols := fwtRd.fwtSch.Sch.GetAllCols()
	numFields := allCols.Size()
	fields := make([]string, numFields)

	i := 0
	offset := 0
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		colWidth := fwtRd.fwtSch.TagToWidth[tag]

		if colWidth > 0 {
			fields[i] = strings.TrimSpace(string(lineBytes[offset : offset+colWidth]))
			offset += colWidth + sepWidth
		}

		i++
		return false
	})

	return untyped.NewRowFromStrings(fwtRd.nbf, fwtRd.GetSchema(), fields), nil
}
