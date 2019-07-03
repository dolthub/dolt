package csv

import (
	"bufio"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// WriteBufSize is the size of the buffer used when writing a csv file.  It is set at the package level and all
// writers create their own buffer's using the value of this variable at the time they create their buffers.
var WriteBufSize = 256 * 1024

// CSVWriter implements TableWriter.  It writes rows as comma separated string values
type CSVWriter struct {
	closer   io.Closer
	bWr      *bufio.Writer
	info     *CSVFileInfo
	delimStr string
	sch      schema.Schema
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
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	delimStr := string(info.Delim)

	if info.HasHeaderLine {
		allCols := outSch.GetAllCols()
		numCols := allCols.Size()
		colNames := make([]string, 0, numCols)
		allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
			colNames = append(colNames, col.Name)
			return false
		})

		headerLine := strings.Join(colNames, delimStr)
		err := iohelp.WriteLine(bwr, headerLine)

		if err != nil {
			wr.Close()
			return nil, err
		}
	}

	return &CSVWriter{wr, bwr, info, delimStr, outSch}, nil
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
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		val, ok := r.GetColVal(tag)
		if ok && !types.IsNull(val) {
			if val.Kind() == types.StringKind {
				colValStrs[i] = string(val.(types.String))
			} else {
				colValStrs[i] = types.EncodedValue(ctx, val)
			}
		}

		i++
		return false
	})

	rowStr := strings.Join(colValStrs, csvw.delimStr)
	err := iohelp.WriteLine(csvw.bWr, rowStr)

	return err
}

// Close should flush all writes, release resources being held
func (csvw *CSVWriter) Close(ctx context.Context) error {
	if csvw.closer != nil {
		errFl := csvw.bWr.Flush()
		errCl := csvw.closer.Close()
		csvw.closer = nil

		if errCl != nil {
			return errCl
		}

		return errFl
	} else {
		return errors.New("Already closed.")
	}
}
