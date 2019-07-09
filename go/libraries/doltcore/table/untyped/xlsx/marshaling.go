package xlsx

import (
	"errors"
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/tealeg/xlsx"
)

func UnmarshalFromXLSX(path string) ([][][]string, error) {
	data, err := xlsx.OpenFile(path)

	if err != nil {
		return nil, err
	}

	dataSlice, err := data.ToSlice()
	if err != nil {
		return nil, err
	}

	return dataSlice, nil
}

func decodeXLSXRows(format *types.Format, xlData [][][]string, sch schema.Schema) ([]row.Row, error) {
	var rows []row.Row

	var err error

	cols := sch.GetAllCols()
	numSheets := len(xlData)
	dataVals := xlData[0]
	header := dataVals[0]
	numRows := len(dataVals) - 1

	taggedVals := make(row.TaggedValues, len(header))

	for j := 0; j < numSheets; j++ {
		for i := 0; i < numRows; i++ {
			for k, v := range header {
				col, ok := cols.GetByName(v)
				if !ok {
					return nil, errors.New(v + "is not a valid column")
				}
				valString := dataVals[i+1][k]
				taggedVals[col.Tag], err = doltcore.StringToValue(valString, col.Kind)
				if err != nil {
					return nil, err
				}
			}
			rows = append(rows, row.New(format, sch, taggedVals))
			fmt.Println(rows)
		}

	}
	return rows, nil
}

func getXlsxRows(path string, tblName string) ([][][]string, error) {
	data, err := xlsx.OpenFile(path)

	if err != nil {
		return nil, err
	}

	var rows [][]string
	var allRows [][][]string
	for _, sheet := range data.Sheets {
		if sheet.Name == tblName {
			for i := 0; i < len(sheet.Rows); i++ {
				var rowVals []string
				for j := 0; j < len(sheet.Rows[i].Cells); j++ {

					rowVals = append(rowVals, sheet.Rows[i].Cells[j].Value)
				}
				rows = append(rows, rowVals)
			}
			allRows = append(allRows, rows)
			return allRows, nil
		}

	}
	return nil, errors.New("table name must match excel sheet name.")
}
