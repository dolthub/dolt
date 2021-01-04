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
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

// writeBufSize is the size of the buffer used when writing a csv file.  It is set at the package level and all
// writers create their own buffer's using the value of this variable at the time they create their buffers.
const writeBufSize = 256 * 1024

// CSVWriter implements TableWriter.  It writes rows as comma separated string values
type CSVWriter struct {
	wr      *bufio.Writer
	closer  io.Closer
	info    *CSVFileInfo
	sch     schema.Schema
	useCRLF bool // True to use \r\n as the line terminator
}

// OpenCSVWriter creates a file at the given path in the given filesystem and writes out rows based on the Schema,
// and CSVFileInfo provided
func OpenCSVWriter(path string, fs filesys.WritableFS, outSch schema.Schema, info *CSVFileInfo) (*CSVWriter, error) {
	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path, os.ModePerm)

	if err != nil {
		return nil, err
	}

	return NewCSVWriter(wr, outSch, info)
}

// NewCSVWriter writes rows to the given WriteCloser based on the Schema and CSVFileInfo provided
func NewCSVWriter(wr io.WriteCloser, outSch schema.Schema, info *CSVFileInfo) (*CSVWriter, error) {

	csvw := &CSVWriter{
		wr:     bufio.NewWriterSize(wr, writeBufSize),
		closer: wr,
		info:   info,
		sch:    outSch,
	}

	if info.HasHeaderLine {
		colNames := make([]*string, 0, outSch.GetAllCols().Size())
		err := outSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			nm := col.Name
			colNames = append(colNames, &nm)
			return false, nil
		})

		if err != nil {
			wr.Close()
			return nil, err
		}

		err = csvw.write(colNames)

		if err != nil {
			wr.Close()
			return nil, err
		}
	}

	return csvw, nil
}

// GetSchema gets the schema of the rows that this writer writes
func (csvw *CSVWriter) GetSchema() schema.Schema {
	return csvw.sch
}

// WriteRow will write a row to a table
func (csvw *CSVWriter) WriteRow(ctx context.Context, r row.Row) error {
	allCols := csvw.sch.GetAllCols()

	colValStrs := make([]*string, 0, allCols.Size())
	_, err := r.IterSchema(csvw.sch, func(tag uint64, val types.Value) (stop bool, err error) {
		val, ok := r.GetColVal(tag)
		if !ok || types.IsNull(val) {
			colValStrs = append(colValStrs, nil)
			return false, nil
		}

		var v string
		if val.Kind() == types.StringKind {
			v = string(val.(types.String))
		} else {
			v, err = types.EncodedValue(ctx, val)
			if err != nil {
				return false, err
			}
		}
		colValStrs = append(colValStrs, &v)

		return false, nil
	})

	if err != nil {
		return err
	}

	return csvw.write(colValStrs)
}

// Close should flush all writes, release resources being held
func (csvw *CSVWriter) Close(ctx context.Context) error {
	if csvw.wr != nil {
		_ = csvw.wr.Flush()
		errCl := csvw.closer.Close()
		csvw.wr = nil
		return errCl
	} else {
		return errors.New("Already closed.")
	}
}

func (csvw *CSVWriter) write(record []*string) error {
	return WriteCSVRow(csvw.wr, record, csvw.info.Delim, csvw.useCRLF)
}

// WriteCSVRow is directly copied from csv.Writer.Write() with the addition of the `isNull []bool` parameter
// this method has been adapted for Dolt's special quoting logic, ie `10,,""` -> (10,NULL,"")
func WriteCSVRow(wr *bufio.Writer, record []*string, delim string, useCRLF bool) error {
	for n, field := range record {
		if n > 0 {
			if _, err := wr.WriteString(delim); err != nil {
				return err
			}
		}

		if field == nil {
			if _, err := wr.WriteString(""); err != nil {
				return err
			}
			continue
		}

		// If we don't have to have a quoted field then just
		// write out the field and continue to the next field.
		if !fieldNeedsQuotes(field, delim) {
			if _, err := wr.WriteString(*field); err != nil {
				return err
			}
			continue
		}

		if err := wr.WriteByte('"'); err != nil {
			return err
		}
		for len(*field) > 0 {
			// Search for special characters.
			i := strings.IndexAny(*field, "\"\r\n")
			if i < 0 {
				i = len(*field)
			}

			// Copy verbatim everything before the special character.
			if _, err := wr.WriteString((*field)[:i]); err != nil {
				return err
			}
			*field = (*field)[i:]

			// Encode the special character.
			if len(*field) > 0 {
				var err error
				switch (*field)[0] {
				case '"':
					_, err = wr.WriteString(`""`)
				case '\r':
					if !useCRLF {
						err = wr.WriteByte('\r')
					}
				case '\n':
					if useCRLF {
						_, err = wr.WriteString("\r\n")
					} else {
						err = wr.WriteByte('\n')
					}
				}
				*field = (*field)[1:]
				if err != nil {
					return err
				}
			}
		}
		if err := wr.WriteByte('"'); err != nil {
			return err
		}
	}
	var err error
	if useCRLF {
		_, err = wr.WriteString("\r\n")
	} else {
		err = wr.WriteByte('\n')
	}

	wr.Buffered()

	return err
}

// Below is the method comment from csv.Writer.fieldNeedsQuotes. It is relevant
// to Dolt's quoting logic for NULLs and ""s, and for import/export compatibility
//
// 		fieldNeedsQuotes reports whether our field must be enclosed in quotes.
// 		Fields with a Comma, fields with a quote or newline, and
// 		fields which start with a space must be enclosed in quotes.
// 		We used to quote empty strings, but we do not anymore (as of Go 1.4).
// 		The two representations should be equivalent, but Postgres distinguishes
// 		quoted vs non-quoted empty string during database imports, and it has
// 		an option to force the quoted behavior for non-quoted CSV but it has
// 		no option to force the non-quoted behavior for quoted CSV, making
// 		CSV with quoted empty strings strictly less useful.
// 		Not quoting the empty string also makes this package match the behavior
// 		of Microsoft Excel and Google Drive.
// 		For Postgres, quote the data terminating string `\.`.
//
func fieldNeedsQuotes(field *string, delim string) bool {
	if field != nil && *field == "" {
		// special Dolt logic
		return true
	}

	// TODO: This is the offending line!
	if *field == `\.` || strings.Contains(*field, delim) || strings.ContainsAny(*field, "\"\r\n") {
		return true
	}

	r1, _ := utf8.DecodeRuneInString(*field)
	return unicode.IsSpace(r1)
}
