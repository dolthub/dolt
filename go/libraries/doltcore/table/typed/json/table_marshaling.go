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

package json

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type JsonRowData map[string]interface{}

type JsonRows struct {
	Rows []map[string]interface{} `json:"rows"`
}

// UnmarshalFromJSON takes a slice of bytes and unmarshals it into a map, where keys of the
// map correspond to the primary key(s).
func UnmarshalFromJSON(data []byte) (*JsonRows, error) {
	var jRows JsonRows
	err := json.Unmarshal(data, &jRows)

	if err != nil {
		return nil, err
	}

	return &jRows, nil
}

// decodeJsonRows takes []JsonRows and converts them to []row.Row
//Need schema and tagged vals to create a new row
func (jr JsonRows) decodeJSONRows(nbf *types.NomsBinFormat, sch schema.Schema) ([]row.Row, error) {
	rowMaps := jr.Rows
	numRows := len(rowMaps)
	rows := make([]row.Row, numRows)
	var err error

	for i := 0; i < numRows; i++ {
		rows[i], err = convToRow(nbf, sch, rowMaps[i])
		if err != nil {
			return nil, err
		}
	}
	return rows, nil

}

//TableFromJSON takes a filepath, vrw, and schema and returns a dolt table.
func TableFromJSON(ctx context.Context, fp string, vrw types.ValueReadWriter, sch schema.Schema) (*doltdb.Table, error) {
	data, err := filesys.LocalFS.ReadFile(fp)
	if err != nil {
		return nil, err
	}

	jsonRows, err := UnmarshalFromJSON(data)
	if err != nil {
		return nil, err
	}

	tblRows, err := jsonRows.decodeJSONRows(vrw.Format(), sch)
	if err != nil {
		return nil, err
	}

	var rowMap types.Map
	m, err := types.NewMap(ctx, vrw)

	if err != nil {
		return nil, err
	}

	me := m.Edit()

	for _, row := range tblRows {
		me = me.Set(row.NomsMapKey(sch), row.NomsMapValue(sch))
	}

	rowMap, err = me.Map(ctx)

	if err != nil {
		return nil, err
	}

	schemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return nil, err
	}

	return doltdb.NewTable(ctx, vrw, schemaVal, rowMap)
}

func convToRow(nbf *types.NomsBinFormat, sch schema.Schema, rowMap map[string]interface{}) (row.Row, error) {
	allCols := sch.GetAllCols()

	taggedVals := make(row.TaggedValues, 1)

	for k, v := range rowMap {
		col, ok := allCols.GetByName(k)
		if !ok {
			return nil, errors.New("column not found in schema")
		}

		switch val := v.(type) {
		case int:
			f := doltcore.GetConvFunc(types.IntKind, col.Kind)
			taggedVals[col.Tag], _ = f(types.Int(val))
		case string:
			f := doltcore.GetConvFunc(types.StringKind, col.Kind)
			taggedVals[col.Tag], _ = f(types.String(val))
		case bool:
			f := doltcore.GetConvFunc(types.BoolKind, col.Kind)
			taggedVals[col.Tag], _ = f(types.Bool(val))
		case float64:
			f := doltcore.GetConvFunc(types.FloatKind, col.Kind)
			taggedVals[col.Tag], _ = f(types.Float(val))
		}

	}
	return row.New(nbf, sch, taggedVals)
}
