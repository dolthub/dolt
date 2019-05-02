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

const colorRowProp = "color"

const diffColTag = schema.ReservedTagMin


type ColorFunc func(string, ...interface{}) string

type ColorDiffSink struct {
	sch schema.Schema
	ttw *tabular.TextTableWriter
}

func NewColorDiffWriter(wr io.WriteCloser, sch schema.Schema) *ColorDiffSink {
	_, additionalCols := untyped.NewUntypedSchemaWithFirstTag(diffColTag, "diff")
	outSch, err := untyped.UntypedSchemaUnion(additionalCols, sch)
	if err != nil {
		panic(err)
	}

	ttw := tabular.NewTextTableWriter(wr, outSch)
	return &ColorDiffSink{outSch, ttw}
}

// GetSchema gets the schema of the rows that this writer writes
func (cds *ColorDiffSink) GetSchema() schema.Schema {
	return cds.sch
}

var colDiffColors = map[DiffChType]ColorFunc{
	DiffAdded:       color.GreenString,
	DiffModifiedOld: color.YellowString,
	DiffModifiedNew: color.YellowString,
	DiffRemoved:     color.RedString,
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
	colorColumns := false
	if prop, ok := props.Get(DiffTypeProp); ok {
		if dt, convertedOK := prop.(DiffChType); convertedOK {
			switch dt {
			case DiffAdded:
				taggedVals[diffColTag] = types.String(" + ")
			case DiffRemoved:
				taggedVals[diffColTag] = types.String(" - ")
			case DiffModifiedOld:
				taggedVals[diffColTag] = types.String(" < ")
			case DiffModifiedNew:
				taggedVals[diffColTag] = types.String(" > ")
				colorColumns = true
			}
		}
	}

	// Color all the columns as appropriate
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		if colorColumns {
			if dt, ok := colDiffs[col.Name]; ok {
				if colorFunc, ok := colDiffColors[dt]; ok {
					taggedVals[tag] = types.String(colorFunc(string(taggedVals[tag].(types.String))))
				}
			}
		} else {
			if prop, ok := props.Get(colorRowProp); ok {
				colorFunc, convertedOK := prop.(func(string, ...interface{}) string)
				if convertedOK {
					taggedVals[tag] = types.String(colorFunc(string(taggedVals[tag].(types.String))))
				}
			}
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
