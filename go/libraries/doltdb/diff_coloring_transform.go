package doltdb

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
)

var greenTextProp = map[string]interface{}{ColorRowProp: color.GreenString}
var redTextProp = map[string]interface{}{ColorRowProp: color.RedString}

func ColoringTransform(row *table.Row) ([]*table.TransformedRowResult, string) {
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

	return []*table.TransformedRowResult{{RowData: rowData, Properties: props}}, ""
}
