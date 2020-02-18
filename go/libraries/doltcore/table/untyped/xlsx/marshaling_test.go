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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestDecodeXLSXRows(t *testing.T) {

	colNames := []string{"id", "first", "last", "age"}
	_, sch := untyped.NewUntypedSchema(colNames...)

	second := [][][]string{}
	first := [][]string{{"id", "first", "last", "age"}, {"1", "osheiza", "otori", "24"}}
	second = append(second, first)

	decoded, err := decodeXLSXRows(types.Format_7_18, second, sch)
	if err != nil {
		fmt.Println(err)

	}

	taggedVals := make(row.TaggedValues, sch.GetAllCols().Size())
	str := "1"
	taggedVals[uint64(0)], _ = typeinfo.StringDefaultType.ParseValue(&str)
	str = "osheiza"
	taggedVals[uint64(1)], _ = typeinfo.StringDefaultType.ParseValue(&str)
	str = "otori"
	taggedVals[uint64(2)], _ = typeinfo.StringDefaultType.ParseValue(&str)
	str = "24"
	taggedVals[uint64(3)], _ = typeinfo.StringDefaultType.ParseValue(&str)

	newRow, err := row.New(types.Format_7_18, sch, taggedVals)

	assert.NoError(t, err)

	if !reflect.DeepEqual(decoded[0], newRow) {
		t.Log("error!")
	}
}

func TestGetRows(t *testing.T) {
	path := "test_files/employees.xlsx"
	stateCols, _ := getXlsxRows(path, "states")
	employeeCols, _ := getXlsxRows(path, "employees")

	if stateCols != nil || employeeCols == nil {
		t.Fatal("error")
	}
}
