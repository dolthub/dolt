package untyped

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// NewUntypedSchema takes an array of field names and returns a schema where the fields use the provided names, are of
// kind types.StringKind, and are not required.
func NewUntypedSchema(colNames ...string) (map[string]uint64, schema.Schema) {
	return NewUntypedSchemaWithFirstTag(0, colNames...)
}

func NewUntypedSchemaWithFirstTag(firstTag uint64, colNames ...string) (map[string]uint64, schema.Schema) {
	cols := make([]schema.Column, len(colNames))
	nameToTag := make(map[string]uint64, len(colNames))

	for i, name := range colNames {
		tag := uint64(i) + firstTag
		// We need at least one primary key col, so choose the first one
		isPk := i == 0
		cols[i] = schema.NewColumn(name, tag, types.StringKind, isPk)
		nameToTag[name] = tag
	}

	colColl, _ := schema.NewColCollection(cols...)
	sch := schema.SchemaFromCols(colColl)

	return nameToTag, sch
}

// NewRowFromStrings is a utility method that takes a schema for an untyped row, and a slice of strings and uses the strings
// as the field values for the row by converting them to noms type.String
func NewRowFromStrings(sch schema.Schema, valStrs []string) row.Row {
	allCols := sch.GetAllCols()

	taggedVals := make(row.TaggedValues)
	for i, valStr := range valStrs {
		tag := uint64(i)
		_, ok := allCols.GetByTag(tag)

		if !ok {
			panic("")
		}

		taggedVals[tag] = types.String(valStr)
	}

	return row.New(sch, taggedVals)
}

// NewRowFromTaggedStrings takes an untyped schema and a map of column tag to string value and returns a row
func NewRowFromTaggedStrings(sch schema.Schema, taggedStrs map[uint64]string) row.Row {
	taggedVals := make(row.TaggedValues)
	for tag, valStr := range taggedStrs {
		taggedVals[tag] = types.String(valStr)
	}

	return row.New(sch, taggedVals)
}

// UntypeSchema takes a schema and returns a schema with the same columns, but with the types of each of those columns
// as types.StringKind
func UntypeSchema(sch schema.Schema) schema.Schema {
	var cols []schema.Column
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		col.Kind = types.StringKind
		cols = append(cols, col)
		return false
	})

	colColl, _ := schema.NewColCollection(cols...)

	return schema.SchemaFromCols(colColl)
}

// UnkeySchema takes a schema and returns a schema with the same columns and types, but stripped of constraints and
// primary keys. Meant for use in result sets.
func UnkeySchema(sch schema.Schema) schema.Schema {
	var cols []schema.Column
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		col.IsPartOfPK = false
		col.Constraints = nil
		cols = append(cols, col)
		return false
	})

	colColl, _ := schema.NewColCollection(cols...)

	return schema.UnkeyedSchemaFromCols(colColl)
}

// UntypeUnkeySchema takes a schema and returns a schema with the same columns, but stripped of constraints and primary
// keys and using only string types. Meant for displaying output and tests.
func UntypeUnkeySchema(sch schema.Schema) schema.Schema {
	var cols []schema.Column
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		col.Kind = types.StringKind
		col.IsPartOfPK = false
		col.Constraints = nil
		cols = append(cols, col)
		return false
	})

	colColl, _ := schema.NewColCollection(cols...)

	return schema.UnkeyedSchemaFromCols(colColl)
}

// UntypedSchemaUnion takes an arbitrary number of schemas and provides the union of all of their key and non-key columns.
// The columns will all be of type types.StringKind and and IsPartOfPK will be false for every column, and all of the
// columns will be in the schemas non-key ColumnCollection.
func UntypedSchemaUnion(schemas ...schema.Schema) (schema.Schema, error) {
	unionSch, err := typed.TypedSchemaUnion(schemas...)

	if err != nil {
		return nil, err
	}

	return UntypeSchema(unionSch), nil
}
