package untyped

import (
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
)

// NewUntypedSchema takes an array of field names and returns a schema where the fields use the provided names, are of
// kind types.StringKind, and are not required.
func NewUntypedSchema(fieldNames []string) *schema.Schema {
	fields := make([]*schema.Field, len(fieldNames))

	for i, fieldName := range fieldNames {
		fields[i] = schema.NewField(fieldName, types.StringKind, false)
	}

	return schema.NewSchema(fields)
}

func TypedToUntypedSchema(typedSch *schema.Schema) *schema.Schema {
	fldNames := make([]string, typedSch.NumFields())
	for i := 0; i < typedSch.NumFields(); i++ {
		fld := typedSch.GetField(i)
		fldNames[i] = fld.NameStr()
	}

	untypedSch := NewUntypedSchema(fldNames)

	for i := 0; i < typedSch.TotalNumConstraints(); i++ {
		cnt := typedSch.GetConstraint(i)
		untypedSch.AddConstraint(cnt)
	}

	return untypedSch
}

func TypedToUntypedMapping(typedSch *schema.Schema) *schema.FieldMapping {
	untypedSch := TypedToUntypedSchema(typedSch)
	destToSrc := make([]int, typedSch.NumFields())
	for i := 0; i < typedSch.NumFields(); i++ {
		destToSrc[i] = i
	}
	return &schema.FieldMapping{DestToSrc: destToSrc, SrcSch: typedSch, DestSch: untypedSch}
}

func TypedToUntypedRowConverter(typedSch *schema.Schema) (*table.RowConverter, error) {
	mapping := TypedToUntypedMapping(typedSch)
	return table.NewRowConverter(mapping)
}

func UntypedSchemaUnion(schemas ...*schema.Schema) *schema.Schema {
	allCols := set.NewStrSet([]string{})
	var ordered []string

	for _, sch := range schemas {
		if sch == nil {
			continue
		}

		for i := 0; i < sch.NumFields(); i++ {
			fld := sch.GetField(i)
			nameStr := fld.NameStr()

			if !allCols.Contains(nameStr) {
				allCols.Add(nameStr)
				ordered = append(ordered, nameStr)
			}
		}
	}

	return NewUntypedSchema(ordered)
}

// NewRowFromStrings is a utility method that takes a schema for an untyped row, and a slice of strings and uses the strings
// as the field values for the row by converting them to noms type.String
func NewRowFromStrings(sch *schema.Schema, fieldValStrs []string) *table.Row {
	fieldVals := make([]types.Value, len(fieldValStrs))
	for i, fieldName := range fieldValStrs {
		fieldVals[i] = types.String(fieldName)
	}

	return table.NewRow(table.RowDataFromValues(sch, fieldVals))
}
