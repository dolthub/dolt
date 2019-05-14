package diff

import (
	"context"
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/tabular"
	"io"
)

const (
	colorRowProp = "color"
 	diffColTag = schema.ReservedTagMin
 	diffColName = "__diff__"
)

type ColorFunc func(string, ...interface{}) string

type ColorDiffSink struct {
	sch schema.Schema
	ttw *tabular.TextTableWriter
}

func NewColorDiffWriter(wr io.WriteCloser, sch schema.Schema) *ColorDiffSink {
	_, additionalCols := untyped.NewUntypedSchemaWithFirstTag(diffColTag, diffColName)
	outSch, err := untyped.UntypedSchemaUnion(additionalCols, sch)
	if err != nil {
		panic(err)
	}

	ttw := tabular.NewTextTableWriterWithNumHeaderRows(wr, outSch, 2)
	return &ColorDiffSink{outSch, ttw}
}

// GetSchema gets the schema of the rows that this writer writes
func (cds *ColorDiffSink) GetSchema() schema.Schema {
	return cds.sch
}

var colDiffColors = map[DiffChType]ColorFunc{
	DiffAdded:       color.New(color.Bold, color.FgGreen).Sprintf,
	DiffModifiedOld: color.RedString,
	DiffModifiedNew: color.GreenString,
	DiffRemoved:     color.New(color.Bold, color.FgRed).Sprintf,
}

func (cds *ColorDiffSink) ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error {

	taggedVals := make(row.TaggedValues)
	allCols := cds.sch.GetAllCols()
	colDiffs := make(map[string]DiffChType)

	if prop, ok := props.Get(CollChangesProp); ok {
		if convertedVal, convertedOK := prop.(map[string]DiffChType); convertedOK {
			colDiffs = convertedVal
		}
	}

	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		if val, ok := r.GetColVal(tag); ok {
			taggedVals[tag] = val.(types.String)
		}
		return false
	})

	taggedVals[diffColTag] = types.String("   ")
	colorColumns := true
	if prop, ok := props.Get(DiffTypeProp); ok {
		if dt, convertedOK := prop.(DiffChType); convertedOK {
			switch dt {
			case DiffAdded:
				taggedVals[diffColTag] = types.String(" + ")
				colorColumns = false
			case DiffRemoved:
				taggedVals[diffColTag] = types.String(" - ")
				colorColumns = false
			case DiffModifiedOld:
				taggedVals[diffColTag] = types.String(" < ")
			case DiffModifiedNew:
				taggedVals[diffColTag] = types.String(" > ")
			}
			// Treat the diff indicator string as a diff of the same type
			colDiffs[diffColName] = dt
		}
	}

	// Color the columns as appropriate. Some rows will be all colored.
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		var colorFunc ColorFunc
		if colorColumns {
			if dt, ok := colDiffs[col.Name]; ok {
				if fn, ok := colDiffColors[dt]; ok {
					colorFunc = fn
				}
			}
		} else {
			if prop, ok := props.Get(DiffTypeProp); ok {
				if dt, convertedOK := prop.(DiffChType); convertedOK {
					if fn, ok := colDiffColors[dt]; ok {
						colorFunc = fn
					}
				}
			}
		}

		if colorFunc != nil {
			taggedVals[tag] = types.String(colorFunc(string(taggedVals[tag].(types.String))))
		}

		return false
	})

	r = row.New(cds.sch, taggedVals)
	return cds.ttw.WriteRow(context.TODO(), r)
}

// Close should release resources being held
func (cds *ColorDiffSink) Close() error {
	if cds.ttw != nil {
		if err := cds.ttw.Close(context.TODO()); err != nil {
			return err
		}
		cds.ttw = nil
		return nil
	} else {
		return errors.New("Already closed.")
	}
}
