// Copyright 2019 Dolthub, Inc.
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
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

// ReadBufSize is the size of the buffer used when reading the csv file.  It is set at the package level and all
// readers create their own buffer's using the value of this variable at the time they create their buffers.
var ReadBufSize = 256 * 1024

// CSVReader implements TableReader.  It reads csv files and returns rows.
type CSVReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	sch    schema.Schema
	isDone bool
	nbf    *types.NomsBinFormat

	// CSV parsing is based on the standard Golang csv parser in encoding/csv/reader.go
	// This parser has been adapted to differentiate between quoted and unquoted
	// empty strings, and to use multi-rune delimiters. This adaptation removes the
	// comment feature and the lazyQuotes option
	delim           []byte
	numLine         int
	fieldsPerRecord int
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
	if len(info.Delim) < 1 {
		return nil, errors.New(fmt.Sprintf("delimiter '%s' has invalid length", info.Delim))
	}
	if !validDelim(info.Delim) {
		return nil, errors.New(fmt.Sprintf("invalid delimiter: %s", string(info.Delim)))
	}

	br := bufio.NewReaderSize(r, ReadBufSize)
	colStrs, err := getColHeaders(br, info)

	if err != nil {
		r.Close()
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	return &CSVReader{
		closer:          r,
		bRd:             br,
		sch:             sch,
		isDone:          false,
		nbf:             nbf,
		delim:           []byte(info.Delim),
		fieldsPerRecord: sch.GetAllCols().Size(),
	}, nil
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
			cols := make([]string, len(colStrsFromFile))
			for i := range colStrsFromFile {
				s := colStrsFromFile[i]
				if s == nil || strings.TrimSpace(*s) == "" {
					return nil, errors.New("bad header line: column cannot be NULL or empty string")
				}
				cols[i] = *s
			}
			colStrs = cols
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

	colVals, err := csvr.csvReadRecords(nil)

	if err == io.EOF {
		csvr.isDone = true
		return nil, io.EOF
	}

	allCols := csvr.sch.GetAllCols()

	if len(colVals) != allCols.Size() {
		var out strings.Builder
		for _, cv := range colVals {
			if cv != nil {
				out.WriteString(*cv)
			}
			out.WriteRune(',')
		}
		return nil, table.NewBadRow(nil,
			fmt.Sprintf("csv reader's schema expects %d fields, but line only has %d values.", allCols.Size(), len(colVals)),
			fmt.Sprintf("line: '%s'", out.String()),
		)
	}

	if err != nil {
		return nil, table.NewBadRow(nil, err.Error())
	}

	taggedVals := make(row.TaggedValues)
	for i := 0; i < allCols.Size(); i++ {
		col := allCols.GetByIndex(i)
		if colVals[i] == nil {
			taggedVals[col.Tag] = nil
			continue
		}
		taggedVals[col.Tag] = types.String(*colVals[i])
	}

	return row.New(csvr.nbf, csvr.sch, taggedVals)
}

func (csvr *CSVReader) ReadSqlRow(crx context.Context) (sql.Row, error) {
	if csvr.isDone {
		return nil, io.EOF
	}

	colVals, err := csvr.csvReadRecords(nil)

	if err == io.EOF {
		csvr.isDone = true
		return nil, io.EOF
	}

	schSize := csvr.sch.GetAllCols().Size()
	if len(colVals) != schSize {
		var out strings.Builder
		for _, cv := range colVals {
			if cv != nil {
				out.WriteString(*cv)
			}
			out.WriteRune(',')
		}
		return nil, table.NewBadRow(nil,
			fmt.Sprintf("csv reader's schema expects %d fields, but line only has %d values.", schSize, len(colVals)),
			fmt.Sprintf("line: '%s'", out.String()),
		)
	}

	if err != nil {
		return nil, table.NewBadRow(nil, err.Error())
	}

	var sqlRow sql.Row
	for _, colVal := range colVals {
		if colVal == nil {
			sqlRow = append(sqlRow, nil)
		} else {
			sqlRow = append(sqlRow, *colVal)
		}
	}

	return sqlRow, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (csvr *CSVReader) GetSchema() schema.Schema {
	return csvr.sch
}

// VerifySchema checks that the in schema matches the original schema
func (csvr *CSVReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(csvr.sch, outSch)
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

// Functions below this line are borrowed or adapted from encoding/csv/reader.go

func validDelim(s string) bool {
	return !(strings.Contains(s, "\"") ||
		strings.Contains(s, "\r") ||
		strings.Contains(s, "\n") ||
		strings.Contains(s, string([]byte{0xFF, 0xFD}))) // Unicode replacement char
}

func lengthNL(b []byte) int {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return 1
	}
	return 0
}

// readLine reads the next line (with the trailing endline).
// If EOF is hit without a trailing endline, it will be omitted.
// If some bytes were read, then the error is never io.EOF.
// The result is only valid until the next call to readLine.
func (csvr *CSVReader) readLine() ([]byte, error) {
	var rawBuffer []byte

	line, err := csvr.bRd.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		rawBuffer = append(rawBuffer[:0], line...)
		for err == bufio.ErrBufferFull {
			line, err = csvr.bRd.ReadSlice('\n')
			rawBuffer = append(rawBuffer, line...)
		}
		line = rawBuffer
	}
	if len(line) > 0 && err == io.EOF {
		err = nil
		// For backwards compatibility, drop trailing \r before EOF.
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	csvr.numLine++
	// Normalize \r\n to \n on all input lines.
	if n := len(line); n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		line[n-2] = '\n'
		line = line[:n-1]
	}
	return line, err
}

type recordState struct {
	line []byte
	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	recordBuffer []byte
	fieldIndexes []int
}

func (csvr *CSVReader) csvReadRecords(dst []*string) ([]*string, error) {
	rs := recordState{}
	recordStartline := csvr.numLine // Starting line for record

	var err error
	for err == nil {
		rs.line, err = csvr.readLine()
		if err == nil && len(rs.line) == lengthNL(rs.line) {
			rs.line = nil
			continue // Skip empty lines
		}
		break
	}
	if err == io.EOF {
		return nil, err
	}

	// nullString indicates whether to interpret an empty string as a NULL
	// only empty strings escaped with double quotes will be non-null
	nullString := make(map[int]bool)
	fieldIdx := 0

	kontinue := true
	for kontinue {
		// Parse each field in the record.
		rs.line = bytes.TrimLeftFunc(rs.line, unicode.IsSpace)
		keep := true
		if len(rs.line) == 0 || rs.line[0] != '"' {
			kontinue, keep, err = csvr.parseField(&rs)
			if !keep {
				nullString[fieldIdx] = true
			}
		} else {
			kontinue, err = csvr.parseQuotedField(&rs)
		}
		fieldIdx++
	}

	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(rs.recordBuffer) // Convert to string once to batch allocations
	dst = dst[:0]
	if cap(dst) < len(rs.fieldIndexes) {
		dst = make([]*string, len(rs.fieldIndexes))
	}
	dst = dst[:len(rs.fieldIndexes)]
	var preIdx int
	for i, idx := range rs.fieldIndexes {
		_, ok := nullString[i]
		if ok {
			dst[i] = nil
		} else {
			s := str[preIdx:idx]
			dst[i] = &s
		}
		preIdx = idx
	}

	// Check or update the expected fields per record.
	if csvr.fieldsPerRecord > 0 {
		if len(dst) != csvr.fieldsPerRecord && err == nil {
			err = &csv.ParseError{StartLine: recordStartline, Line: csvr.numLine, Err: csv.ErrFieldCount}
		}
	} else if csvr.fieldsPerRecord == 0 {
		csvr.fieldsPerRecord = len(dst)
	}

	return dst, err
}

func (csvr *CSVReader) parseField(rs *recordState) (kontinue bool, keep bool, err error) {
	i := bytes.Index(rs.line, csvr.delim)
	field := rs.line
	if i >= 0 {
		field = field[:i]
	} else {
		field = field[:len(field)-lengthNL(field)]
	}
	rs.recordBuffer = append(rs.recordBuffer, field...)
	rs.fieldIndexes = append(rs.fieldIndexes, len(rs.recordBuffer))
	keep = len(field) != 0 // discard unquoted empty strings
	if i >= 0 {
		dl := len(csvr.delim)
		rs.line = rs.line[i+dl:]
		return true, keep, err
	}
	return false, keep, err
}

func (csvr *CSVReader) parseQuotedField(rs *recordState) (kontinue bool, err error) {
	const quoteLen = len(`"`)
	dl := len(csvr.delim)
	recordStartLine := csvr.numLine
	fullLine := rs.line

	// Quoted string field
	rs.line = rs.line[quoteLen:]
	for {
		i := bytes.IndexByte(rs.line, '"')
		if i >= 0 {
			// Hit next quote.
			rs.recordBuffer = append(rs.recordBuffer, rs.line[:i]...)
			rs.line = rs.line[i+quoteLen:]

			atDelimiter := len(rs.line) >= dl && bytes.Compare(rs.line[:dl], csvr.delim) == 0
			nextRune, _ := utf8.DecodeRune(rs.line)

			switch {
			case atDelimiter:
				// `"<delimiter>` sequence (end of field).
				rs.line = rs.line[dl:]
				rs.fieldIndexes = append(rs.fieldIndexes, len(rs.recordBuffer))
				return true, err
			case nextRune == '"':
				// `""` sequence (append quote).
				rs.recordBuffer = append(rs.recordBuffer, '"')
				rs.line = rs.line[quoteLen:]
			case lengthNL(rs.line) == len(rs.line):
				// `"\n` sequence (end of line).
				rs.fieldIndexes = append(rs.fieldIndexes, len(rs.recordBuffer))
				return false, err
			default:
				// `"*` sequence (invalid non-escaped quote).
				col := utf8.RuneCount(fullLine[:len(fullLine)-len(rs.line)-quoteLen])
				err = &csv.ParseError{StartLine: recordStartLine, Line: csvr.numLine, Column: col, Err: csv.ErrQuote}
				return false, err
			}
		} else if len(rs.line) > 0 {
			// Hit end of line (copy all data so far).
			rs.recordBuffer = append(rs.recordBuffer, rs.line...)
			if err != nil {
				return false, err
			}
			rs.line, err = csvr.readLine()
			if err == io.EOF {
				err = nil
			}
			fullLine = rs.line
		} else {
			// Abrupt end of file
			if err == nil {
				col := utf8.RuneCount(fullLine)
				err = &csv.ParseError{StartLine: recordStartLine, Line: csvr.numLine, Column: col, Err: csv.ErrQuote}
				return false, err
			}
			rs.fieldIndexes = append(rs.fieldIndexes, len(rs.recordBuffer))
			return false, err
		}
	}
}
