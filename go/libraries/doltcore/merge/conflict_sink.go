package merge

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/diff"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
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

var diffTypeToColor = map[types.DiffChangeType]diff.ColorFunc{
	types.DiffChangeAdded:    color.GreenString,
	types.DiffChangeModified: color.YellowString,
	types.DiffChangeRemoved:  deleteColor.Sprintf,
}

type ConflictSink struct {
	bWr        *bufio.Writer
	sch        schema.Schema
	colSep     string
	inFieldCnt int
}

func NewConflictSink(wr io.Writer, sch schema.Schema, colSep string) *ConflictSink {
	_, additionalCols := untyped.NewUntypedSchemaWithFirstTag(schema.ReservedTagMin, "op", "branch")
	outSch, err := untyped.UntypedSchemaUnion(additionalCols, sch)

	if err != nil {
		panic(err)
	}

	bWr := bufio.NewWriterSize(wr, WriteBufSize)
	return &ConflictSink{bWr, outSch, colSep, sch.GetAllCols().Size()}
}

// GetSchema gets the schema of the rows that this writer writes
func (cWr *ConflictSink) GetSchema() schema.Schema {
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
func (cWr *ConflictSink) ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error {
	numFields := cWr.sch.GetAllCols().Size()
	colStrs := make([]string, numFields)

	colorFunc := noColorFunc
	mergeVersion, _ := props.Get(mergeVersionProp)
	colStrs[0] = "   "
	colStrs[1] = string(mergeVersionToLabel[mergeVersion.(MergeVersion)])

	if mergeVersion != BaseVersion {
		mergeRowOp, _ := props.Get(mergeRowOperation)
		dt := mergeRowOp.(types.DiffChangeType)
		colStrs[0] = diffTypeToOpLabel[dt]
		colorFunc = diffTypeToColor[dt]
	}

	i := 0
	cWr.sch.GetAllCols().ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
		if val, ok := r.GetColVal(tag); ok {
			str := string(val.(types.String))
			colStrs[i] = str
		}
		i++
		return false
	})

	lineStr := strings.Join(colStrs, cWr.colSep) + "\n"
	coloredLine := colorFunc(lineStr)
	err := iohelp.WriteAll(cWr.bWr, []byte(coloredLine))

	return err
}

// Close should release resources being held
func (cWr *ConflictSink) Close() error {
	if cWr.bWr != nil {
		cWr.bWr.Flush()
		cWr.bWr = nil

		return nil
	} else {
		return errors.New("already closed")
	}
}
