package untyped

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed"
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
		cols[i] = schema.NewColumn(name, tag, types.StringKind, false)
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

func NewRowFromTaggedStrings(sch schema.Schema, taggedStrs map[uint64]string) row.Row {
	taggedVals := make(row.TaggedValues)
	for tag, valStr := range taggedStrs {
		taggedVals[tag] = types.String(valStr)
	}

	return row.New(sch, taggedVals)
}

func UntypeSchema(sch schema.Schema) schema.Schema {
	var cols []schema.Column
	sch.GetAllCols().ItrUnsorted(func(tag uint64, col schema.Column) (stop bool) {
		col.Kind = types.StringKind
		cols = append(cols, col)
		return false
	})

	colColl, _ := schema.NewColCollection(cols...)

	return schema.SchemaFromCols(colColl)

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
