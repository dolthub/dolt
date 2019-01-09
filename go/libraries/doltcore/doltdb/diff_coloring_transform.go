package doltdb

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

var greenTextProp = map[string]interface{}{ColorRowProp: color.GreenString}
var redTextProp = map[string]interface{}{ColorRowProp: color.RedString}

func ColoringTransform(row *table.Row) ([]*pipeline.TransformedRowResult, string) {
	var props map[string]interface{} = nil
	rowData := row.CurrData()

	diffType, ok := row.GetProperty(DiffTypeProp)

	if ok {
		ct, ok := diffType.(types.DiffChangeType)

		if ok {

			switch ct {
			case types.DiffChangeAdded:
				props = greenTextProp
			case types.DiffChangeRemoved:
				props = redTextProp
			case types.DiffChangeModified:
			}
		}
	}

	return []*pipeline.TransformedRowResult{{RowData: rowData, Properties: props}}, ""
}
