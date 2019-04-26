package resultset

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

// A result set schema understands how to map values from multiple schemas together into a final result schema.
type ResultSetSchema struct {
	mapping        map[schema.Schema]*rowconv.FieldMapping
	destSch        schema.Schema
	maxSchTag      uint64
	maxAssignedTag uint64
}

// Creates a new result set schema for the destination schema given. Successive calls to AddSchema will flesh out the
// mapping values for all source schemas.
func NewFromDestSchema(sch schema.Schema) (*ResultSetSchema, error) {
	maxTag, err := validateSchema(sch)
	if err != nil {
		return nil, err
	}
	return &ResultSetSchema{
		mapping:   make(map[schema.Schema]*rowconv.FieldMapping),
		destSch:   sch,
		maxSchTag: maxTag,
	}, nil
}

// Creates an identity result set schema, to be used for intermediate result sets that haven't yet been compressed.
func Identity(sch schema.Schema) *ResultSetSchema {
	fieldMapping := rowconv.IdentityMapping(sch)
	return &ResultSetSchema{
		mapping:   map[schema.Schema]*rowconv.FieldMapping{sch: fieldMapping},
		destSch:   sch,
	}
}

// Creates a new result set schema for the source schemas given and fills in schema values as necessary.
func NewFromSourceSchemas(sourceSchemas ...schema.Schema) (*ResultSetSchema, error) {
	var sch schema.Schema
	var rss *ResultSetSchema
	var err error

	if sch, err = ConcatSchemas(sourceSchemas...); err != nil {
		return nil, err
	}

	if rss, err = NewFromDestSchema(sch); err != nil {
		return nil, err
	}

	for _, ss := range sourceSchemas {
		if err = rss.AddSchema(ss); err != nil {
			return nil, err
		}
	}

	return rss, nil
}

// Creates a new result set schema for columns given. Tag numbers in the result schema will be rewritten as necessary,
// starting from 0.
func NewFromColumns(columns ...schema.Column) (*ResultSetSchema, error) {
	var sch schema.Schema
	var rss *ResultSetSchema
	var err error

	for i, _ := range columns {
		columns[i].Tag = uint64(i)
	}
	colColl, err := schema.NewColCollection(columns...)
	if err != nil {
		return nil, err
	}

	sch = schema.UnkeyedSchemaFromCols(colColl)

	if rss, err = NewFromDestSchema(sch); err != nil {
		return nil, err
	}

	return rss, nil
}

// SubsetSchema returns a schema that is a subset of the schema given, with keys and constraints removed. Column names
// must be verified before subsetting. Unrecognized column names will cause a panic.
func SubsetSchema(sch schema.Schema, colNames ...string) schema.Schema {
	srcColls := sch.GetAllCols()

	var cols []schema.Column
	for _, name := range colNames {
		if col, ok := srcColls.GetByName(name); !ok {
			panic("Unrecognized name " + name)
		} else {
			cols = append(cols, col)
		}
	}
	colColl, _ := schema.NewColCollection(cols...)
	return schema.UnkeyedSchemaFromCols(colColl)
}

// Validates that the given schema is suitable for use as a result set. Result set schemas must use contiguous tags
// starting at 0.
func validateSchema(sch schema.Schema) (uint64, error) {
	valid := true
	expectedTag := uint64(0)
	sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
		if tag != expectedTag {
			valid = false
			return true
		}
		expectedTag++
		return false
	})

	if !valid {
		return 0, errors.New("Result set mappings must use contiguous tag numbers starting at 0")
	}

	return expectedTag - 1, nil
}

// Adds a schema to the result set mapping
func (rss *ResultSetSchema) AddSchema(sch schema.Schema) error {
	if rss.maxAssignedTag + uint64(len(sch.GetAllCols().GetColumns()) - 1) > rss.maxSchTag {
		return errors.New("No room for additional schema in mapping, result set schema too small")
	}

	fieldMapping := make(map[uint64]uint64)
	sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
		fieldMapping[tag] = rss.maxAssignedTag
		rss.maxAssignedTag++
		return false
	})

	mapping, err := rowconv.NewFieldMapping(sch, rss.destSch, fieldMapping)
	if err != nil {
		return err
	}

	rss.mapping[sch] = mapping
	return nil
}

// Adds a single column to the result set schema, from the source schema given.
func (rss *ResultSetSchema) AddColumn(sourceSchema schema.Schema, column schema.Column) error {
	if rss.maxAssignedTag > rss.maxSchTag {
		return errors.New("No room for additional column in mapping, result set schema too small")
	}

	fieldMapping, ok := rss.mapping[sourceSchema]
	if !ok {
		fieldMapping = &rowconv.FieldMapping{SrcSch: sourceSchema, DestSch: rss.destSch, SrcToDest: make(map[uint64]uint64)}
	}
	fieldMapping.SrcToDest[column.Tag] = rss.maxAssignedTag
	rss.maxAssignedTag++

	rss.mapping[sourceSchema] = fieldMapping
	return nil
}

// Schema returns the schema for this result set.
func (rss *ResultSetSchema) Schema() schema.Schema {
	return rss.destSch
}

// Mapping returns the field mapping for the given schema.
func (rss *ResultSetSchema) Mapping(sch schema.Schema) *rowconv.FieldMapping {
	return rss.mapping[sch]
}

// Concatenates the given schemas together into a new one. This rewrites the tag numbers to be contiguous and
// starting from zero, and removes all keys and constraints. Types are preserved.
func ConcatSchemas(srcSchemas ...schema.Schema) (schema.Schema, error) {
	cols := make([]schema.Column, 0)
	var itag uint64
	for _, col := range srcSchemas {
		col.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
			cols = append(cols, schema.NewColumn(col.Name, itag, col.Kind, false))
			itag++
			return false
		})
	}
	colCollection, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}
	return schema.UnkeyedSchemaFromCols(colCollection), nil
}

// Returns the cross-product of the table results given. The returned rows will have the schema of this result set, and
// will have (N * M * ... X) rows, one for every possible combination of entries in the table results given.
func (rss *ResultSetSchema) CrossProduct(tables []TableResult) []row.Row {
	// special case: no tables means no rows
	if len(tables) == 0 {
		return nil
	}
	emptyRow := RowWithSchema{row.New(rss.destSch, row.TaggedValues{}), rss.destSch}
	return rss.cph(emptyRow, tables)
}

// Recursive helper function for CrossProduct
func (rss *ResultSetSchema) cph(r RowWithSchema, tables []TableResult) []row.Row {
	if len(tables) == 0 {
		return []row.Row{r.Row}
	}

	resultSet := make([]row.Row, 0)
	table := tables[0]
	for _, r2 := range table.Rows {
		partialRow := rss.CombineRows(r, RowWithSchema{r2, table.Schema})
		resultSet = append(resultSet, rss.cph(partialRow, tables[1:])...)
	}
	return resultSet
}

// CombineRows writes all values from r2 into r1 and returns it. r1 must have the same schema as the result set.
func (rss *ResultSetSchema) CombineRows(r1 RowWithSchema, r2 RowWithSchema) RowWithSchema {
	if !schema.SchemasAreEqual(r1.Schema, rss.destSch) {
		panic("Cannot call CombineRows on a row with a different schema than the result set schema")
	}

	fieldMapping, ok := rss.mapping[r2.Schema]
	if !ok {
		panic (fmt.Sprintf("Unrecognized schema %v", r1.Schema))
	}

	r2.Row.IterCols(func(tag uint64, val types.Value) (stop bool) {
		var err error

		err = r1.SetColVal(fieldMapping.SrcToDest[tag], val)
		if err != nil {
			panic(err.Error())
		}
		return false
	})
	return r1
}

// CombineRows writes all values from other rows into r1 and returns it. r1 must have the same schema as the result set.
func (rss *ResultSetSchema) CombineAllRows(r1 RowWithSchema, rows ...RowWithSchema) RowWithSchema {
	for _, r2 := range rows {
		r1 = rss.CombineRows(r1, r2)
	}
	return r1
}