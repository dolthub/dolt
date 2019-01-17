package merge

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"strings"
)

var WriteBufSize = 256 * 1024
var mergeVersionToLabel = map[MergeVersion]string{
	OurVersion:   "ours  ",
	TheirVersion: "theirs",
	BaseVersion:  "base  ",
}
var diffTypeToOpLabel = map[types.DiffChangeType]string{
	types.DiffChangeAdded:    " + ",
	types.DiffChangeRemoved:  " - ",
	types.DiffChangeModified: " * ",
}

var deleteColor = color.New(color.FgRed, color.CrossedOut)

var diffTypeToColor = map[types.DiffChangeType]doltdb.ColorFunc{
	types.DiffChangeAdded:    color.GreenString,
	types.DiffChangeModified: color.YellowString,
	types.DiffChangeRemoved:  deleteColor.Sprintf,
}

type ConflictWriter struct {
	bWr        *bufio.Writer
	sch        *schema.Schema
	colSep     string
	inFieldCnt int
}

func NewConflictWriter(wr io.Writer, sch *schema.Schema, colSep string) *ConflictWriter {
	additionalCols := untyped.NewUntypedSchema([]string{"op", "branch"})
	outSch := untyped.UntypedSchemaUnion(additionalCols, sch)
	bWr := bufio.NewWriterSize(wr, WriteBufSize)
	return &ConflictWriter{bWr, outSch, colSep, sch.NumFields()}
}

// GetSchema gets the schema of the rows that this writer writes
func (cWr *ConflictWriter) GetSchema() *schema.Schema {
	return cWr.sch
}

var noColorFunc = func(s string, i ...interface{}) string {
	if len(i) == 0 {
		return s
	} else {
		return fmt.Sprintf(s)
	}
}

// WriteRow will write a row to a table
func (cWr *ConflictWriter) WriteRow(row *table.Row) error {
	numFields := cWr.sch.NumFields()
	colStrs := make([]string, numFields)

	colorFunc := noColorFunc
	mergeVersion, _ := row.GetProperty(mergeVersionProp)
	colStrs[0] = "   "
	colStrs[1] = string(mergeVersionToLabel[mergeVersion.(MergeVersion)])

	if mergeVersion != BaseVersion {
		mergeRowOp, _ := row.GetProperty(mergeRowOperation)
		dt := mergeRowOp.(types.DiffChangeType)
		colStrs[0] = diffTypeToOpLabel[dt]
		colorFunc = diffTypeToColor[dt]
	}

	rowData := row.CurrData()
	for i := 0; i < cWr.inFieldCnt; i++ {
		val, _ := rowData.GetField(i)
		str := string(val.(types.String))
		colStrs[i+2] = str
	}

	lineStr := strings.Join(colStrs, cWr.colSep) + "\n"
	coloredLine := colorFunc(lineStr)
	err := iohelp.WriteAll(cWr.bWr, []byte(coloredLine))

	return err
}

// Close should release resources being held
func (cWr *ConflictWriter) Close() error {
	if cWr.bWr != nil {
		cWr.bWr.Flush()
		cWr.bWr = nil

		return nil
	} else {
		return errors.New("already closed")
	}
}
