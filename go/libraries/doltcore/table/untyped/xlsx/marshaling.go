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

package xlsx

import (
	"errors"
	"fmt"

	"github.com/tealeg/xlsx"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
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

func decodeXLSXRows(nbf *types.NomsBinFormat, xlData [][][]string, sch schema.Schema) ([]row.Row, error) {
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
			rows = append(rows, row.New(nbf, sch, taggedVals))
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
