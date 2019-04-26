package json

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"

	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
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
func (jr JsonRows) decodeJSONRows(sch schema.Schema) ([]row.Row, error) {
	rowMaps := jr.Rows
	numRows := len(rowMaps)
	rows := make([]row.Row, numRows)
	var err error

	for i := 0; i < numRows; i++ {
		rows[i], err = convToRow(sch, rowMaps[i])
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

	tblRows, err := jsonRows.decodeJSONRows(sch)
	if err != nil {
		return nil, err
	}

	var rowMap types.Map
	me := types.NewMap(context.TODO(), vrw).Edit()
	for _, row := range tblRows {
		me = me.Set(row.NomsMapKey(sch), row.NomsMapValue(sch))
	}
	rowMap = me.Map(context.TODO())

	schemaVal, err := encoding.MarshalAsNomsValue(vrw, sch)
	if err != nil {
		return nil, err
	}

	tbl := doltdb.NewTable(ctx, vrw, schemaVal, rowMap)
	if err != nil {
		return nil, err
	}

	return tbl, nil

}

func convToRow(sch schema.Schema, rowMap map[string]interface{}) (row.Row, error) {
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
	return row.New(sch, taggedVals), nil
}
