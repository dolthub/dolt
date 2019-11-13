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

package main

import (
	"io"
	"log"
	"strings"
)

// Dataset is a set of test data used for benchmark testing
type Dataset interface {
	//// String returns a string formatted based on the Dataset's SeedSchema
	//String() string
	//
	//// Bytes returns a byte slice formatted based on the Dataset's SeedSchema
	//Bytes() []byte

	GenerateData()

	// Change returns a Dataset mutated by the given percentage of change
	Change(pct float32) Dataset
}

// DSImpl implements the Dataset interface
type DSImpl struct {
	// Schema defines the structure of the Dataset
	Schema *SeedSchema

	// TableName is the name of the test dataset
	TableName string

	wc io.Writer
}

// NewDSImpl creates a new DSImpl
func NewDSImpl(wc io.Writer, sch *SeedSchema, tableName string) *DSImpl {
	return &DSImpl{Schema: sch, TableName: tableName, wc: wc}
}

//// String returns a string of the dataset formatted based on the DSImpl's Schema
//func (ds *DSImpl) String() {
//	generateTestData(ds.wc, ds.Schema.Rows, ds.Schema.Columns, ds.TableName, ds.Schema.FileFormatExt)
//}
//

func (ds *DSImpl) GenerateData() {
	generateTestData(ds.wc, ds.Schema.Rows, ds.Schema.Columns, ds.TableName, ds.Schema.FileFormatExt)
}

// Change returns a DataSet that is a mutation of this Dataset by the given percentage
func (ds *DSImpl) Change(pct float32) Dataset {
	// TODO
	return &DSImpl{}
}

func generateTestData(wc io.Writer, rows int, cols []*SeedColumn, tableName, format string) {
	var offset int
	switch format {
	case csvExt:
		offset = 1
	default:
		offset = 0
	}
	//strs := getRowStrs(rows, offset, cols, tableName, format)
	//return formatDataStr(strs, cols, tableName, format)
	writeDataToWriter(wc, rows, offset, cols, tableName, format)
}

//func getRowStrs(w io.Writer, rows, offset int, cols []*SeedColumn, tableName, format string) {
func writeDataToWriter(wc io.Writer, rows, offset int, cols []*SeedColumn, tableName, format string) {
	//strs := make([]string, rows+offset)

	// handle the "header" for all format types
	switch format {
	case csvExt:
		header := makeHeaderStr(cols, tableName, format)
		_, err := wc.Write([]byte(header + "\n"))
		if err != nil {
			log.Fatal(err)
		}
	case sqlExt:
		header := getSQLHeader(cols, tableName, format)
		_, err := wc.Write([]byte(header + "\n"))
		if err != nil {
			log.Fatal(err)
		}
	case jsonExt:
		prefix := "{\"Rows\":["
		_, err := wc.Write([]byte(prefix))
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unable to write the header, unsupported format %v \n", format)
	}

	// writeHeader(w, cols, tableName, format)

	var prevRow []string
	for i := 0; i < rows; i++ {
		row := make([]string, len(cols))

		for colIndex, col := range cols {
			val := getColValue(prevRow, colIndex, col, format)
			row[colIndex] = val

			if i > 0 && prevRow != nil {
				prevRow[colIndex] = val
			}
		}

		//tempR := formatRow(row, cols, tableName, format)
		//if format == jsonExt {
		//	if i == rows - 1 {
		//		tempR = tempR + "\n"
		//	} else {
		//		tempR = tempR + ",\n"
		//	}
		//} else {
		//	tempR = tempR + "\n"
		//}
		_, err := wc.Write([]byte(formatRow(row, cols, i, rows-1, tableName, format)))
		if err != nil {
			log.Fatal(err)
		}
		prevRow = row[:]
	}

	// handle the "footer" for all format types
	switch format {
	case jsonExt:
		suffix := "]}\n"
		_, err := wc.Write([]byte(suffix))
		if err != nil {
			log.Fatal(err)
		}
	default:
	}
}

func formatRow(strs []string, cols []*SeedColumn, currentRowIdx, lastRowIdx int, tableName, format string) string {
	switch format {
	case csvExt:
		return strings.Join(strs, ",") + "\n"
	case sqlExt:
		return getSQLRow(strs, cols, tableName) + "\n"
	case jsonExt:
		var suffix string
		if currentRowIdx == lastRowIdx {
			suffix = "\n"
		} else {
			suffix = ",\n"
		}
		return getJSONRow(strs, cols) + suffix
	default:
		log.Fatalf("cannot format row, unsupported file format %s \n", format)
	}
	return ""
}
//
//func formatDataStr(strs []string, cols []*SeedColumn, tableName, format string) string {
//	switch format {
//	case csvExt:
//		return strings.Join(strs, "\n")
//	case sqlExt:
//		return formatSQLStr(strs, cols, tableName, format)
//	case jsonExt:
//		return formatJSONStr(strs, cols, tableName, format)
//	default:
//		log.Fatalf("cannot format data string, unsupported file format %s \n", format)
//	}
//	return ""
//}

func makeHeaderStr(cols []*SeedColumn, tableName, format string) string {
	str := make([]string, 0, len(cols))
	for _, col := range cols {
		str = append(str, col.Name)
	}
	return formatRow(str, cols, 0, 1, tableName, format)
}
