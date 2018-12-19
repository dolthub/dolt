package table

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
)

// RowData is a tuple of data following a given schema
type RowData struct {
	sch       *schema.Schema
	fieldVals []types.Value
}

func RowDataFromValues(sch *schema.Schema, fieldVals []types.Value) *RowData {
	numFieldVals := len(fieldVals)
	lastValid := -1
	for i := numFieldVals - 1; i >= 0; i-- {
		if !types.IsNull(fieldVals[i]) {
			lastValid = i
			break
		}
	}

	for i := 0; i < lastValid; i++ {
		if types.IsNull(fieldVals[i]) {
			fieldVals[i] = types.NullValue
		}
	}

	var rowFields []types.Value
	if lastValid == -1 {
		rowFields = make([]types.Value, 0)
	} else {
		rowFields = fieldVals[:lastValid+1]
	}

	return &RowData{sch, rowFields}
}

func RowDataFromPKAndValueList(sch *schema.Schema, pk types.Value, fieldValueList types.List) *RowData {
	pkIndex := sch.GetPKIndex()
	fieldValues := make([]types.Value, 0, fieldValueList.Len()+1)
	for i := 0; uint64(i) < fieldValueList.Len(); i++ {
		if i == pkIndex {
			fieldValues = append(fieldValues, pk)
		}

		fieldValues = append(fieldValues, fieldValueList.Get(uint64(i)))
	}

	if pkIndex == int(fieldValueList.Len()) {
		fieldValues = append(fieldValues, pk)
	}

	return RowDataFromValues(sch, fieldValues)
}
func RowDataFromValMap(sch *schema.Schema, vals map[string]types.Value) *RowData {
	fieldValues := make([]types.Value, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		f := sch.GetField(i)
		val, _ := vals[f.NameStr()]
		fieldValues[i] = val
	}

	return RowDataFromValues(sch, fieldValues)
}

func RowDataFromUntypedMap(sch *schema.Schema, vals map[string]string) (rowData *RowData, firstBad *string) {
	fieldValues := make([]types.Value, sch.NumFields())
	for i := 0; i < sch.NumFields(); i++ {
		f := sch.GetField(i)
		fldName := f.NameStr()
		val, ok := vals[fldName]

		if ok {
			convFunc := GetConvFunc(types.StringKind, f.NomsKind())
			converted, err := convFunc(types.String(val))

			if err != nil {
				return nil, &fldName
			}

			fieldValues[i] = converted
		}
	}

	return RowDataFromValues(sch, fieldValues), nil
}

// GetSchema gets the schema for the row
func (rd *RowData) GetSchema() *schema.Schema {
	return rd.sch
}

// GetField will return a field's value along with the Field definition and whether it is the primary key by index.
func (rd *RowData) GetField(index int) (val types.Value, field *schema.Field) {
	if index < 0 || index >= rd.sch.NumFields() {
		panic("Index out of range")
	}

	if index < len(rd.fieldVals) {
		val = rd.fieldVals[index]
	}

	f := rd.sch.GetField(index)
	return val, f
}

// GetFieldByName will return a field's value along with the Field definition and whether it is the primary key by
// field name.
func (rd *RowData) GetFieldByName(name string) (types.Value, *schema.Field) {
	index := rd.sch.GetFieldIndex(name)
	return rd.GetField(index)
}
