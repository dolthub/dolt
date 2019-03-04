package csv

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"strings"
)

var ReadBufSize = 256 * 1024

type CSVReader struct {
	closer io.Closer
	bRd    *bufio.Reader
	info   *CSVFileInfo
	sch    schema.Schema
	isDone bool
}

func OpenCSVReader(path string, fs filesys.ReadableFS, info *CSVFileInfo) (*CSVReader, error) {
	r, err := fs.OpenForRead(path)

	if err != nil {
		return nil, err
	}

	return NewCSVReader(r, info)
}

func NewCSVReader(r io.ReadCloser, info *CSVFileInfo) (*CSVReader, error) {
	br := bufio.NewReaderSize(r, ReadBufSize)
	colStrs, err := getColHeaders(br, info)

	if err != nil {
		r.Close()
		return nil, err
	}

	_, sch := untyped.NewUntypedSchema(colStrs...)

	return &CSVReader{r, br, info, sch, false}, nil
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

		colStrsFromFile := csvSplitLine(line, info.Delim, info.EscapeQuotes)

		if colStrs == nil {
			colStrs = colStrsFromFile
		}
	}

	return colStrs, nil
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (csvr *CSVReader) ReadRow() (row.Row, error) {
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
func (csvr *CSVReader) Close() error {
	if csvr.closer != nil {
		err := csvr.closer.Close()
		csvr.closer = nil

		return err
	} else {
		return errors.New("Already closed.")
	}
}

func (csvr *CSVReader) parseRow(line string) (row.Row, error) {
	colVals := csvSplitLine(line, csvr.info.Delim, csvr.info.EscapeQuotes)

	sch := csvr.sch
	numCols := sch.GetAllCols().Size()
	if len(colVals) != numCols {
		return nil, table.NewBadRow(nil,
			fmt.Sprintf("csv reader's schema expects %d fields, but line only has %d values.", numCols, len(colVals)),
			fmt.Sprintf("line: %s", line),
		)
	}

	return untyped.NewRowFromStrings(sch, colVals), nil
}
