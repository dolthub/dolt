package diff

import (
	"bufio"
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"path/filepath"
	"strings"
)

const (
	ColorRowProp = "color"
)

type ColorFunc func(string, ...interface{}) string

var WriteBufSize = 256 * 1024

type ColorDiffSink struct {
	closer io.Closer
	bWr    *bufio.Writer
	sch    schema.Schema
	colSep string
}

func OpenColorDiffSink(path string, fs filesys.WritableFS, sch schema.Schema, colSep string) (*ColorDiffSink, error) {
	err := fs.MkDirs(filepath.Dir(path))

	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)

	if err != nil {
		return nil, err
	}

	return NewColorDiffWriter(wr, sch, colSep), nil
}

func NewColorDiffWriter(wr io.Writer, sch schema.Schema, colSep string) *ColorDiffSink {
	bwr := bufio.NewWriterSize(wr, WriteBufSize)
	return &ColorDiffSink{nil, bwr, sch, colSep}
}

// GetSchema gets the schema of the rows that this writer writes
func (cdWr *ColorDiffSink) GetSchema() schema.Schema {
	return cdWr.sch
}

var colDiffColors = map[DiffChType]ColorFunc{
	DiffAdded:       color.GreenString,
	DiffModifiedOld: color.YellowString,
	DiffModifiedNew: color.YellowString,
	DiffRemoved:     color.RedString,
}

// WriteRow will write a row to a table
func (cdWr *ColorDiffSink) ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error {
	allCols := cdWr.sch.GetAllCols()
	colStrs := make([]string, allCols.Size())
	colDiffs := make(map[string]DiffChType)
	if prop, ok := props.Get(CollChangesProp); ok {
		if convertedVal, convertedOK := prop.(map[string]DiffChType); convertedOK {
			colDiffs = convertedVal
		}
	}

	i := 0
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		val, _ := r.GetColVal(tag)
		str := string(val.(types.String))
		colStrs[i] = str

		i++
		return false
	})

	prefix := "   "
	colorColumns := false
	if prop, ok := props.Get(DiffTypeProp); ok {
		if dt, convertedOK := prop.(DiffChType); convertedOK {
			switch dt {
			case DiffAdded:
				prefix = " + "
			case DiffRemoved:
				prefix = " - "
			case DiffModifiedOld:
				prefix = " < "
			case DiffModifiedNew:
				prefix = " > "
				colorColumns = true
			}
		}
	}

	if colorColumns {
		i = 0
		allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
			if dt, ok := colDiffs[col.Name]; ok {
				if colorFunc, ok := colDiffColors[dt]; ok {
					colStrs[i] = colorFunc(colStrs[i])
				}
			}

			i++
			return false
		})
	}

	lineStr := prefix + strings.Join(colStrs, cdWr.colSep) + "\n"

	if !colorColumns {
		if prop, ok := props.Get(ColorRowProp); ok {
			colorer, convertedOK := prop.(func(string, ...interface{}) string)
			if convertedOK {
				lineStr = colorer(lineStr)
			}
		}
	}

	err := iohelp.WriteAll(cdWr.bWr, []byte(lineStr))

	return err
}

// Close should release resources being held
func (cdWr *ColorDiffSink) Close() error {
	if cdWr.bWr != nil {
		errFl := cdWr.bWr.Flush()
		cdWr.bWr = nil

		if cdWr.closer != nil {
			errCl := cdWr.closer.Close()
			cdWr.closer = nil

			if errCl != nil {
				return errCl
			}
		}

		return errFl
	} else {
		return errors.New("Already closed.")
	}
}
