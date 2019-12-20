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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
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
	taggedVals[uint64(0)], _ = doltcore.StringToValue("1", types.StringKind)
	taggedVals[uint64(1)], _ = doltcore.StringToValue("osheiza", types.StringKind)
	taggedVals[uint64(2)], _ = doltcore.StringToValue("otori", types.StringKind)
	taggedVals[uint64(3)], _ = doltcore.StringToValue("24", types.StringKind)

	newRow, err := row.New(types.Format_7_18, sch, taggedVals)

	assert.NoError(t, err)

	if !reflect.DeepEqual(decoded[0], newRow) {
		t.Log("error!")
	}
}

func TestGetRows(t *testing.T) {
	path := "testdata/employees.xlsx"
	stateCols, _ := getXlsxRows(path, "states")
	employeeCols, _ := getXlsxRows(path, "employees")

	if stateCols != nil || employeeCols == nil {
		t.Fatal("error")
	}
}
