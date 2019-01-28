package table

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

var ErrInvalidRow = errors.New("invalid row")

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

func RowDataFromPKAndValueList(sch *schema.Schema, pk types.Value, fieldValueList types.Tuple) *RowData {
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
			convFunc := doltcore.GetConvFunc(types.StringKind, f.NomsKind())
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

func (rd *RowData) CopyValues(dest []types.Value, offset, count int) {
	for i := 0; i < count; i++ {
		dest[i] = rd.fieldVals[offset+i]
	}
}

func (rd *RowData) IsValid() bool {
	sch := rd.GetSchema()
	pkIndex := sch.GetPKIndex()
	for i := 0; i < sch.NumFields(); i++ {
		val, fld := rd.GetField(i)

		if types.IsNull(val) {
			if fld.IsRequired() || i == pkIndex {
				return false
			}
		} else if val.Kind() != fld.NomsKind() {
			return false
		}
	}

	return true
}

// GetFieldByName will return a field's value along with the Field definition and whether it is the primary key by
// field name.
func (rd *RowData) GetFieldByName(name string) (types.Value, *schema.Field) {
	index := rd.sch.GetFieldIndex(name)
	return rd.GetField(index)
}

func (rd *RowData) UpdatedCopy(updates []types.Value) *RowData {
	updatedVals := make([]types.Value, len(rd.fieldVals))

	i := 0
	for ; i < len(updates); i++ {
		newVal := updates[i]

		if newVal == nil {
			updatedVals[i] = rd.fieldVals[i]
		} else {
			updatedVals[i] = newVal
		}
	}

	for ; i < len(rd.fieldVals); i++ {
		updatedVals[i] = rd.fieldVals[i]
	}

	return &RowData{rd.sch, updatedVals}
}
