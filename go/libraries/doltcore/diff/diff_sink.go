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

package diff

import (
	"context"
	"errors"
	"io"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	colorRowProp = "color"
	diffColTag   = schema.ReservedTagMin
	diffColName  = "__diff__"
)

type ColorFunc func(...interface{}) string

type ColorDiffSink struct {
	sch schema.Schema
	ttw *tabular.TextTableWriter
}

// NewColorDiffSink returns a ColorDiffSink that uses  the writer and schema given to print its output. numHeaderRows
// will change how many rows of output are considered part of the table header. Use 1 for diffs where the schemas are
// the same between the two table revisions, and 2 for when they differ.
func NewColorDiffSink(wr io.WriteCloser, sch schema.Schema, numHeaderRows int) (*ColorDiffSink, error) {
	_, additionalCols := untyped.NewUntypedSchemaWithFirstTag(diffColTag, diffColName)
	outSch, err := untyped.UntypedSchemaUnion(additionalCols, sch)
	if err != nil {
		panic(err)
	}

	ttw, err := tabular.NewTextTableWriterWithNumHeaderRows(wr, outSch, numHeaderRows)

	if err != nil {
		return nil, err
	}

	return &ColorDiffSink{outSch, ttw}, nil
}

// GetSchema gets the schema of the rows that this writer writes
func (cds *ColorDiffSink) GetSchema() schema.Schema {
	return cds.sch
}

var colDiffColors = map[DiffChType]ColorFunc{
	DiffAdded:       color.New(color.Bold, color.FgGreen).Sprint,
	DiffModifiedOld: color.New(color.FgRed).Sprint,
	DiffModifiedNew: color.New(color.FgGreen).Sprint,
	DiffRemoved:     color.New(color.Bold, color.FgRed).Sprint,
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

	err := allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if val, ok := r.GetColVal(tag); ok {
			taggedVals[tag] = val.(types.String)
		}
		return false, nil
	})

	if err != nil {
		return err
	}

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
	err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
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

		return false, nil
	})

	if err != nil {
		return err
	}

	r, err = row.New(r.Format(), cds.sch, taggedVals)

	if err != nil {
		return err
	}

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
