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

type SQLDiffSink struct {
	sch schema.Schema
	sw  *sqlexport.SqlExportWriter
}

func NewSQLDiffSink(wr io.WriteCloser, sch schema.Schema, typedSch schema.Schema, tableName string) (*SQLDiffSink, error) {
	sw, err := sqlexport.NewSQLExportWriter(wr, tableName, typedSch)

	if err != nil {
		return nil, err
	}
	sw.SetWrittenFirstRow(true)

	return &SQLDiffSink{sch, sw}, nil
}

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
			taggedVals[tag] = val
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
	if prop, ok := props.Get(DiffTypeProp); ok {
		if dt, convertedOK := prop.(DiffChType); convertedOK {
			switch dt {
			case DiffAdded:
				return sds.sw.WriteInsertRow(context.TODO(), r)
			case DiffRemoved:
				return sds.sw.WriteDeleteRow(context.TODO(), r)
			case DiffModifiedOld:
				return nil
			case DiffModifiedNew:
				return sds.sw.WriteUpdateRow(context.TODO(), r)
			}
			// Treat the diff indicator string as a diff of the same type
			colDiffs[diffColName] = dt
		}
	}

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
