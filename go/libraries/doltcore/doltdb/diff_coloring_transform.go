package doltdb

import (
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
)

var greenTextProp = map[string]interface{}{ColorRowProp: color.GreenString}
var redTextProp = map[string]interface{}{ColorRowProp: color.RedString}
var yellowTextProp = map[string]interface{}{ColorRowProp: color.YellowString}

func ColoringTransform(row *table.Row) ([]*pipeline.TransformedRowResult, string) {
	var props map[string]interface{} = nil
	rowData := row.CurrData()

	diffType, ok := row.GetProperty(DiffTypeProp)

	if ok {
		ct, ok := diffType.(DiffChType)

		if ok {

			switch ct {
			case DiffAdded:
				props = greenTextProp
			case DiffRemoved:
				props = redTextProp
			case DiffModifiedOld:
				props = yellowTextProp
			case DiffModifiedNew:
				props = yellowTextProp
			}
		}
	}

	return []*pipeline.TransformedRowResult{{RowData: rowData, Properties: props}}, ""
}
