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
	"errors"
	"io"

	"github.com/dolthub/dolt/go/libraries/utils/set"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

type SQLDiffSink struct {
	wr        io.WriteCloser
	sch       schema.Schema
	tableName string
}

// NewSQLDiffSink creates a SQLDiffSink for a diff pipeline.
func NewSQLDiffSink(wr io.WriteCloser, sch schema.Schema, tableName string) (*SQLDiffSink, error) {
	return &SQLDiffSink{wr, sch, tableName}, nil
}

// GetSchema gets the schema that the SQLDiffSink was created with.
func (sds *SQLDiffSink) GetSchema() schema.Schema {
	return sds.sch
}

// ProcRowWithProps satisfies pipeline.SinkFunc; it writes SQL diff statements to output.
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
				stmt, err := sqlfmt.RowAsInsertStmt(r, sds.tableName, sds.sch)

				if err != nil {
					return err
				}

				return iohelp.WriteLine(sds.wr, stmt)
			case DiffRemoved:
				stmt, err := sqlfmt.RowAsDeleteStmt(r, sds.tableName, sds.sch)

				if err != nil {
					return err
				}

				return iohelp.WriteLine(sds.wr, stmt)
			case DiffModifiedOld:
				return nil
			case DiffModifiedNew:
				if len(colDiffs) > 0 {
					// Pass in the update as a setStr
					keys := make([]string, len(colDiffs))

					i := 0
					for k := range colDiffs {
						keys[i] = k
						i++
					}
					stmt, err := sqlfmt.RowAsUpdateStmt(r, sds.tableName, sds.sch, set.NewStrSet(keys))

					if err != nil {
						return err
					}

					return iohelp.WriteLine(sds.wr, stmt)
				}
			}
			// Treat the diff indicator string as a diff of the same type
			colDiffs[diffColName] = dt
		}
	}

	return err
}

// ProcRowWithProps satisfies pipeline.SinkFunc; it writes rows as SQL statements.
func (sds *SQLDiffSink) ProcRowForExport(r row.Row, _ pipeline.ReadableMap) error {
	stmt, err := sqlfmt.RowAsInsertStmt(r, sds.tableName, sds.sch)

	if err != nil {
		return err
	}

	return iohelp.WriteLine(sds.wr, stmt)
}

// Close should release resources being held
func (sds *SQLDiffSink) Close() error {
	if sds.wr != nil {
		err := sds.wr.Close()
		if err != nil {
			return err
		}
		sds.wr = nil
		return nil
	} else {
		return errors.New("Already closed.")
	}
}
