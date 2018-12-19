package doltdb

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"regexp"
)

const (
	tableStructName = "table"

	schemaRefKey = "schema_ref"
	tableRowsKey = "rows"

	// TableNameRegexStr is the regular expression that valid tables must match.
	TableNameRegexStr = `^[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
)

var tableNameRegex, _ = regexp.Compile(TableNameRegexStr)

// IsValidTableName returns true if the name matches the regular expression `[0-9a-z]+[-_0-9a-z]*[0-9a-z]+$`
func IsValidTableName(name string) bool {
	return tableNameRegex.MatchString(name)
}

// Table is a struct which holds row data, as well as a reference to it's schema.
type Table struct {
	vrw         types.ValueReadWriter
	tableStruct types.Struct
}

// NewTable creates a noms Struct which stores the schema and the row data
func NewTable(vrw types.ValueReadWriter, schema types.Value, rowData types.Map) *Table {
	schemaRef := writeValAndGetRef(vrw, schema)
	rowDataRef := writeValAndGetRef(vrw, rowData)

	sd := types.StructData{
		schemaRefKey: schemaRef,
		tableRowsKey: rowDataRef,
	}

	tableStruct := types.NewStruct(tableStructName, sd)
	return &Table{vrw, tableStruct}
}

// GetSchema will retrieve the schema being referenced from the table in noms and unmarshal it.
func (t *Table) GetSchema(vrw types.ValueReadWriter) *schema.Schema {
	schemaRefVal := t.tableStruct.Get(schemaRefKey)
	schemaRef := schemaRefVal.(types.Ref)
	schemaVal := schemaRef.TargetValue(vrw)

	schema, _ := noms.UnmarshalNomsValue(schemaVal)

	return schema
}

// HasTheSameSchema tests the schema within 2 tables for equality
func (t *Table) HasTheSameSchema(t2 *Table) bool {
	schemaVal := t.tableStruct.Get(schemaRefKey)
	schemaRef := schemaVal.(types.Ref)

	schema2Val := t2.tableStruct.Get(schemaRefKey)
	schema2Ref := schema2Val.(types.Ref)

	return schemaRef.TargetHash() == schema2Ref.TargetHash()
}

// GetRow uses the noms Map containing the row data to lookup a row by primary key.  If a valid row exists with this pk
// then the supplied TableRowFactory will be used to create a TableRow using the row data.
func (t *Table) GetRow(pk types.Value, sch *schema.Schema) (row *table.Row, exists bool) {
	rowMap := t.GetRowData()
	fieldsVal := rowMap.Get(pk)

	if fieldsVal == nil {
		return nil, false
	}

	return table.NewRow(table.RowDataFromPKAndValueList(sch, pk, fieldsVal.(types.List))), true
}

// ValueItr defines a function that iterates over a collection of noms values.  The ValueItr will return a valid value
// and true until all the values in the collection are exhausted.  At that time nil and false will be returned.
type ValueItr func() (val types.Value, ok bool)

// ValueSliceItr returns a closure that has the signature of a ValueItr and can be used to iterate over a slice of values
func ValueSliceItr(vals []types.Value) func() (types.Value, bool) {
	next := 0
	size := len(vals)
	return func() (types.Value, bool) {
		current := next
		next++

		if current < size {
			return vals[current], true
		}

		return nil, false
	}
}

// SetItr returns a closure that has the signature of a ValueItr and can be used to iterate over a noms Set of vaules
func SetItr(valSet types.Set) func() (types.Value, bool) {
	itr := valSet.Iterator()
	return func() (types.Value, bool) {
		v := itr.Next()
		return v, v != nil
	}
}

// GetRows takes in a ValueItr which will supply a stream of primary keys to be pulled from the table.  Each key is
// looked up sequentially.  If row data exists for a given pk it is converted to a TableRow, and added to the rows
// slice. If row data does not exist for a given pk it will be added to the missing slice.  The numPKs argument, if
// known helps allocate the right amount of memory for the results, but if the number of pks being requested isn't
// known then 0 can be used.
func (t *Table) GetRows(pkItr ValueItr, numPKs int, sch *schema.Schema) (rows []*table.Row, missing []types.Value) {
	if numPKs < 0 {
		numPKs = 0
	}

	rows = make([]*table.Row, 0, numPKs)
	missing = make([]types.Value, 0, numPKs)

	rowMap := t.GetRowData()

	for pk, ok := pkItr(); ok; pk, ok = pkItr() {
		fieldsVal := rowMap.Get(pk)

		if fieldsVal == nil {
			missing = append(missing, pk)
		} else {
			row := table.NewRow(table.RowDataFromPKAndValueList(sch, pk, fieldsVal.(types.List)))
			rows = append(rows, row)
		}
	}

	return rows, missing
}

// UpdateRows replaces the current row data and returns and updated Table.  Calls to UpdateRows will not be written to the
// database.  The root must be updated with the updated table, and the root must be committed or written.
func (t *Table) UpdateRows(updatedRows types.Map) *Table {
	rowDataRef := writeValAndGetRef(t.vrw, updatedRows)
	updatedSt := t.tableStruct.Set(tableRowsKey, rowDataRef)

	return &Table{t.vrw, updatedSt}
}

// GetRowData retrieves the underlying map which is a map from a primary key to a list of field values.
func (t *Table) GetRowData() types.Map {
	rowMapRef := t.tableStruct.Get(tableRowsKey).(types.Ref)
	rowMap := rowMapRef.TargetValue(t.vrw).(types.Map)
	return rowMap
}
