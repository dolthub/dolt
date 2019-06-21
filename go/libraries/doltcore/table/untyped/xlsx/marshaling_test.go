package xlsx

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/store/types"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
)

func TestDecodeXLSXRows(t *testing.T) {

	colNames := []string{"id", "first", "last", "age"}
	_, sch := untyped.NewUntypedSchema(colNames...)

	second := [][][]string{}
	first := [][]string{{"id", "first", "last", "age"}, {"1", "osheiza", "otori", "24"}}
	second = append(second, first)

	decoded, err := decodeXLSXRows(second, sch)
	if err != nil {
		fmt.Println(err)

	}

	taggedVals := make(row.TaggedValues, sch.GetAllCols().Size())
	taggedVals[uint64(0)], _ = doltcore.StringToValue("1", types.StringKind)
	taggedVals[uint64(1)], _ = doltcore.StringToValue("osheiza", types.StringKind)
	taggedVals[uint64(2)], _ = doltcore.StringToValue("otori", types.StringKind)
	taggedVals[uint64(3)], _ = doltcore.StringToValue("24", types.StringKind)

	newRow := row.New(sch, taggedVals)

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
