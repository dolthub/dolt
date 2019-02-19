package doltdb

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/pantoerr"
	"regexp"
)

const (
	tableStructName = "table"

	schemaRefKey       = "schema_ref"
	tableRowsKey       = "rows"
	conflictsKey       = "conflicts"
	conflictSchemasKey = "conflict_schemas"

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

func (t *Table) SetConflicts(schemas Conflict, conflictData types.Map) *Table {
	conflictsRef := writeValAndGetRef(t.vrw, conflictData)

	updatedSt := t.tableStruct.Set(conflictSchemasKey, schemas.ToNomsList(t.vrw))
	updatedSt = updatedSt.Set(conflictsKey, conflictsRef)

	return &Table{t.vrw, updatedSt}
}

func (t *Table) GetConflicts() (Conflict, types.Map, error) {
	schemasVal, ok := t.tableStruct.MaybeGet(conflictSchemasKey)

	if !ok {
		return Conflict{}, types.EmptyMap, ErrNoConflicts
	}

	schemas := ConflictFromTuple(schemasVal.(types.Tuple))
	conflictsVal := t.tableStruct.Get(conflictsKey)

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		confMap = confMapRef.TargetValue(t.vrw).(types.Map)
	}

	return schemas, confMap, nil
}

func (t *Table) HasConflicts() bool {
	_, ok := t.tableStruct.MaybeGet(conflictSchemasKey)

	return ok
}

func (t *Table) NumRowsInConflict() uint64 {
	conflictsVal, ok := t.tableStruct.MaybeGet(conflictsKey)

	if !ok {
		return 0
	}

	confMap := types.EmptyMap
	if conflictsVal != nil {
		confMapRef := conflictsVal.(types.Ref)
		confMap = confMapRef.TargetValue(t.vrw).(types.Map)
	}

	return confMap.Len()
}

func (t *Table) ClearConflicts() *Table {
	tSt := t.tableStruct.Delete(conflictSchemasKey)
	tSt = tSt.Delete(conflictsKey)

	return &Table{t.vrw, tSt}
}

func (t *Table) GetConflictSchemas() (base, sch, mergeSch *schema.Schema, err error) {
	schemasVal, ok := t.tableStruct.MaybeGet(conflictSchemasKey)

	if ok {
		schemas := ConflictFromTuple(schemasVal.(types.Tuple))
		baseRef := schemas.Base.(types.Ref)
		valRef := schemas.Value.(types.Ref)
		mergeRef := schemas.MergeValue.(types.Ref)

		var err error
		var baseSch, sch, mergeSch *schema.Schema
		if baseSch, err = refToSchema(t.vrw, baseRef); err == nil {
			if sch, err = refToSchema(t.vrw, valRef); err == nil {
				mergeSch, err = refToSchema(t.vrw, mergeRef)
			}
		}

		return baseSch, sch, mergeSch, err
	} else {
		return nil, nil, nil, ErrNoConflicts
	}
}

func refToSchema(vrw types.ValueReadWriter, ref types.Ref) (*schema.Schema, error) {
	var schema *schema.Schema
	err := pantoerr.PanicToErrorInstance(ErrNomsIO, func() error {
		schemaVal := ref.TargetValue(vrw)

		var err error
		schema, err = noms.UnmarshalNomsValue(schemaVal)

		if err != nil {
			return err
		}

		return nil
	})

	return schema, err
}

// GetSchema will retrieve the schema being referenced from the table in noms and unmarshal it.
func (t *Table) GetSchema() *schema.Schema {
	schemaRefVal := t.tableStruct.Get(schemaRefKey)
	schemaRef := schemaRefVal.(types.Ref)
	schema, _ := refToSchema(t.vrw, schemaRef)

	return schema
}

func (t *Table) GetSchemaRef() types.Ref {
	return t.tableStruct.Get(schemaRefKey).(types.Ref)
}

// HasTheSameSchema tests the schema within 2 tables for equality
func (t *Table) HasTheSameSchema(t2 *Table) bool {
	schemaVal := t.tableStruct.Get(schemaRefKey)
	schemaRef := schemaVal.(types.Ref)

	schema2Val := t2.tableStruct.Get(schemaRefKey)
	schema2Ref := schema2Val.(types.Ref)

	return schemaRef.TargetHash() == schema2Ref.TargetHash()
}

// HashOf returns the hash of the underlying table struct
func (t *Table) HashOf() hash.Hash {
	return t.tableStruct.Hash()
}

// GetRow uses the noms Map containing the row data to lookup a row by primary key.  If a valid row exists with this pk
// then the supplied TableRowFactory will be used to create a TableRow using the row data.
func (t *Table) GetRow(pk types.Value, sch *schema.Schema) (row *table.Row, exists bool) {
	rowMap := t.GetRowData()
	fieldsVal := rowMap.Get(pk)

	if fieldsVal == nil {
		return nil, false
	}

	return table.NewRow(table.RowDataFromPKAndValueList(sch, pk, fieldsVal.(types.Tuple))), true
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
			row := table.NewRow(table.RowDataFromPKAndValueList(sch, pk, fieldsVal.(types.Tuple)))
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

func (t *Table) ResolveConflicts(keys []string) (invalid, notFound []string, tbl *Table, err error) {
	sch := t.GetSchema()
	pk := sch.GetField(sch.GetPKIndex())
	convFunc := doltcore.GetConvFunc(types.StringKind, pk.NomsKind())

	removed := 0
	_, confData, err := t.GetConflicts()

	if err != nil {
		return nil, nil, nil, err
	}

	confEdit := confData.Edit()

	for _, keyStr := range keys {
		key, err := convFunc(types.String(keyStr))

		if err != nil {
			invalid = append(invalid, keyStr)
		}

		if confEdit.Has(key) {
			removed++
			confEdit.Remove(key)
		} else {
			notFound = append(notFound, keyStr)
		}
	}

	if removed == 0 {
		return invalid, notFound, tbl, nil
	}

	conflicts := confEdit.Map()
	conflictsRef := writeValAndGetRef(t.vrw, conflicts)
	updatedSt := t.tableStruct.Set(conflictsKey, conflictsRef)

	return invalid, notFound, &Table{t.vrw, updatedSt}, nil
}
