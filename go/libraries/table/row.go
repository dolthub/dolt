package table

import (
	"bytes"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
)

type Row struct {
	data       *RowData
	properties map[string]interface{}
}

func NewRow(rd *RowData) *Row {
	return &Row{rd, nil}
}

func NewRowWithProperties(rd *RowData, props map[string]interface{}) *Row {
	return &Row{rd, props}
}

func (row *Row) ClonedMergedProperties(addedProps map[string]interface{}) map[string]interface{} {
	if row.properties == nil {
		return addedProps
	}

	newProps := make(map[string]interface{})
	for k, v := range row.properties {
		newProps[k] = v
	}

	if addedProps != nil {
		for k, v := range addedProps {
			newProps[k] = v
		}
	}

	return newProps
}

func (row *Row) AddProperty(propName string, val interface{}) {
	if row.properties == nil {
		row.properties = map[string]interface{}{propName: val}
	} else {
		row.properties[propName] = val
	}
}

func (row *Row) AddProperties(properties map[string]interface{}) {
	if row.properties != nil {
		row.properties = properties
	} else {
		for k, v := range properties {
			row.properties[k] = v
		}
	}
}

func (row *Row) GetProperty(propName string) (interface{}, bool) {
	val, ok := row.properties[propName]
	return val, ok
}

func (row *Row) CurrData() *RowData {
	return row.data
}

func (row *Row) GetSchema() *schema.Schema {
	return row.data.sch
}

func GetPKFromRow(row *Row) types.Value {
	sch := row.data.GetSchema()
	pkIndex := sch.GetPKIndex()
	val, _ := row.data.GetField(pkIndex)
	return val
}

// GetNonPKFieldListFromRow gets the values for non primary key row fields as a noms list
func GetNonPKFieldListFromRow(row *Row, vrw types.ValueReadWriter) types.List {
	sch := row.data.GetSchema()
	pkIdx := sch.GetPKIndex()

	if pkIdx == -1 {
		panic("Only valid for rows that have primary keys")
	}

	numFields := sch.NumFields()
	nonPKValues := make([]types.Value, numFields-1)
	for i, j := 0, 0; i < numFields; i++ {
		if i != pkIdx {
			nonPKValues[j], _ = row.data.GetField(i)
			j++
		}
	}

	for i := numFields - 2; i >= 0; i-- {
		if nonPKValues[i] == nil {
			nonPKValues = nonPKValues[:i]
		} else {
			break
		}
	}

	return types.NewList(vrw, nonPKValues...)
}

// RowIsValid will return true if every column defined as required in the table's schema is non null
func RowIsValid(row *Row) bool {
	sch := row.data.GetSchema()
	pkIndex := sch.GetPKIndex()
	for i := 0; i < sch.NumFields(); i++ {
		val, fld := row.data.GetField(i)

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

func InvalidFieldsForRow(row *Row) []string {
	sch := row.data.GetSchema()
	badFields := make([]string, 0, sch.NumFields())
	pkIndex := sch.GetPKIndex()
	for i := 0; i < sch.NumFields(); i++ {
		val, fld := row.data.GetField(i)

		if types.IsNull(val) {
			if fld.IsRequired() || i == pkIndex {
				badFields = append(badFields, fld.NameStr())
			}
		} else if val.Kind() != fld.NomsKind() {
			badFields = append(badFields, fld.NameStr())
		}
	}

	return badFields
}

var fieldDelim = []byte(" | ")

// RowsEqualIgnoringSchema will ignore the schema of two rows and will compare field values index by index.
func RowsEqualIgnoringSchema(row, other *Row) bool {
	return RowDataEqualIgnoringSchema(row.CurrData(), other.CurrData())
}

// RowDataEqualIgnoringSchema will ignore the schema of two rows and will compare field values index by index.
func RowDataEqualIgnoringSchema(rowData, other *RowData) bool {
	longer := rowData
	shorter := other

	if rowData.GetSchema().NumFields() > rowData.GetSchema().NumFields() {
		longer = other
		shorter = rowData
	}

	lNumFields := longer.GetSchema().NumFields()
	sNumFields := shorter.GetSchema().NumFields()
	for i := 0; i < lNumFields; i++ {
		lVal, _ := longer.GetField(i)

		var sVal types.Value = types.NullValue
		if i < sNumFields {
			sVal, _ = shorter.GetField(i)
		}

		if types.IsNull(lVal) {
			if !types.IsNull(sVal) {
				return false
			}
		} else {
			if types.IsNull(sVal) || !lVal.Equals(sVal) {
				return false
			}
		}
	}

	return true
}

type RowFormatFunc func(row *Row) string
type RowDataFormatFunc func(row *RowData) string

var RowDataFmt = FieldSeparatedFmt(':')
var RowFmt RowFormatFunc = func(row *Row) string {
	return RowDataFmt(row.CurrData())
}

// String returns the string representation of the row with fields separated by pipes
func FieldSeparatedFmt(delim rune) RowDataFormatFunc {
	return func(row *RowData) string {
		sch := row.GetSchema()
		numFields := sch.NumFields()
		kvps := make([]string, numFields)

		var backingBuffer [512]byte
		buf := bytes.NewBuffer(backingBuffer[:0])
		for i := 0; i < numFields; i++ {
			if i != 0 {
				buf.Write(fieldDelim)
			}

			val, fld := row.GetField(i)
			buf.Write([]byte(fld.NameStr()))
			buf.WriteRune(delim)
			types.WriteEncodedValue(buf, val)
			kvps[i] = buf.String()
		}

		return buf.String()
	}
}

/*var emptyHashSl []byte

func init() {
	emptyHashSl = make([]byte, hash.ByteLen)
	for i := 0; i < hash.ByteLen; i++ {
		emptyHashSl[i] = 0
	}
}

// UUIDForColVals returns a uuid that is the sha1 hash of the concatenated noms hashes of the selected fields.
func (row *Row) UUIDForColVals(fieldIndices []int) uuid.UUID {
	hashBuf := make([]byte, len(fieldIndices)*hash.ByteLen)
	for i, index := range fieldIndices {
		val, _ := row.GetField(index)

		var hashSl []byte
		if val == nil {
			hashSl = emptyHashSl
		} else {
			hashBytes := [hash.ByteLen]byte(val.Hash())
			hashSl = hashBytes[:hash.ByteLen]
		}
		start := i * hash.ByteLen
		end := (i + 1) * hash.ByteLen
		copy(hashBuf[start:end], hashSl)
	}

	return uuid.NewSHA1(uuid.NameSpaceOID, hashBuf)
}*/
