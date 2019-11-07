// Copyright 2019 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/sqlexport"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	//colorRowProp = "color"
	//diffColTag   = schema.ReservedTagMin
	//diffColName  = "__diff__"
)

type SQLDiffSink struct {
	sch schema.Schema
	sw  *sqlexport.SqlExportWriter
}

// NewSQLDiffSink returns a SQLDiffSink that uses  the writer and schema given to print its output. numHeaderRows
// will change how many rows of output are considered part of the table header. Use 1 for diffs where the schemas are
// the same between the two table revisions, and 2 for when they differ.
func NewSQLDiffSink(wr io.WriteCloser, sch schema.Schema, typedSch schema.Schema, tableName string) (*SQLDiffSink, error) {
	sw, err := sqlexport.NewSQLExportWriter(wr, tableName, typedSch)

	if err != nil {
		return nil, err
	}
	sw.SetWrittenFirstRow(true)

	return &SQLDiffSink{sch, sw}, nil
}

// GetSchema gets the schema of the rows that this writer writes
func (sds *SQLDiffSink) GetSchema() schema.Schema {
	return sds.sch
}

func (sds *SQLDiffSink) ProcRowWithProps(r row.Row, props pipeline.ReadableMap) error {

	taggedVals := make(row.TaggedValues)
	allCols := sds.sch.GetAllCols()
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

	r, err = row.New(r.Format(), sds.sch, taggedVals)

	if err != nil {
		return err
	}

	taggedVals[diffColTag] = types.String("   ")
	colorColumns := true
	if prop, ok := props.Get(DiffTypeProp); ok {
		if dt, convertedOK := prop.(DiffChType); convertedOK {
			switch dt {
			case DiffAdded:
				return sds.sw.WriteInsertRow(context.TODO(), r)
				//taggedVals[diffColTag] = types.String(" + ")
			case DiffRemoved:
				return sds.sw.WriteDeleteRow(context.TODO(), r)
				//taggedVals[diffColTag] = types.String(" - ")
			case DiffModifiedOld:
				return nil
				//taggedVals[diffColTag] = types.String(" < ")
			case DiffModifiedNew:
				return nil
				//return sds.sw.WriteUpdateRow(context.TODO(), r)
				//taggedVals[diffColTag] = types.String(" > ")
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

	return err
}

// Close should release resources being held
func (sds *SQLDiffSink) Close() error {
	if sds.sw != nil {
		if err := sds.sw.Close(context.TODO()); err != nil {
			return err
		}
		sds.sw = nil
		return nil
	} else {
		return errors.New("Already closed.")
	}
}
