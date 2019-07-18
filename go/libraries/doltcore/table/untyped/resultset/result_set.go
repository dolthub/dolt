package resultset

import (
	"errors"
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// TODO: fix this hot mess

// A result set schema understands how to map values from multiple schemas together into a final result schema.
type ResultSetSchema struct {
	mapping        map[schema.Schema]*rowconv.FieldMapping
	schemas        map[string]schema.Schema
	destSch        schema.Schema
	maxSchTag      uint64
	maxAssignedTag uint64
}

// Schema returns the schema for this result set.
func (rss *ResultSetSchema) Schema() schema.Schema {
	return rss.destSch
}

// Mapping returns the field mapping for the given schema.
func (rss *ResultSetSchema) Mapping(sch schema.Schema) *rowconv.FieldMapping {
	return rss.mapping[sch]
}

func (rss *ResultSetSchema) ResolveTag(tableName string, columnName string) (uint64, error) {
	sch, ok := rss.schemas[tableName]
	if !ok {
		return schema.InvalidTag, errors.New("cannot find table " + tableName)
	}

	column, ok := sch.GetAllCols().GetByName(columnName)
	if !ok {
		return schema.InvalidTag, errors.New("cannot find column " + columnName)
	}

	mapping := rss.mapping[sch]
	tag, ok := mapping.SrcToDest[column.Tag]
	if !ok {
		return schema.InvalidTag, errors.New("no mapping for column " + columnName)
	}

	return tag, nil
}

// Creates a new result set schema for the destination schema given. Successive calls to AddSchema will flesh out the
// mapping values for all source schemas.
func newFromDestSchema(sch schema.Schema) (*ResultSetSchema, error) {
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

// Creates an identity result set schema, to be used for intermediate result sets that haven't yet been compressed.
func Identity(tableName string, sch schema.Schema) *ResultSetSchema {
	fieldMapping := rowconv.IdentityMapping(sch)
	return &ResultSetSchema{
		mapping: map[schema.Schema]*rowconv.FieldMapping{sch: fieldMapping},
		destSch: sch,
		schemas: map[string]schema.Schema{tableName: sch},
	}
}

// Creates a new result set schema for the source schemas given and fills in schema values as necessary.
func newFromSourceSchemas(sourceSchemas ...schema.Schema) (*ResultSetSchema, error) {
	var sch schema.Schema
	var rss *ResultSetSchema
	var err error

	if sch, err = concatSchemas(sourceSchemas...); err != nil {
		return nil, err
	}

	if rss, err = newFromDestSchema(sch); err != nil {
		return nil, err
	}

	for _, ss := range sourceSchemas {
		if err = rss.addSchema(ss); err != nil {
			return nil, err
		}
	}

	return rss, nil
}

// Adds a schema to the result set mapping
func (rss *ResultSetSchema) addSchema(sch schema.Schema) error {
	if rss.maxAssignedTag+uint64(len(sch.GetAllCols().GetColumns())-1) > rss.maxSchTag {
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

// Creates a new result set schema for columns given. Tag numbers in the result schema will be rewritten as necessary,
// starting from 0.
func NewFromColumns(schemas map[string]schema.Schema, columns ...ColWithSchema) (*ResultSetSchema, error) {
	var sch schema.Schema
	var rss *ResultSetSchema
	var err error

	cols := make([]schema.Column, len(columns))
	for i, _ := range columns {
		col := columns[i].Col
		col.Tag = uint64(i)
		cols[i] = col
	}
	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	sch = schema.UnkeyedSchemaFromCols(colColl)

	if rss, err = newFromDestSchema(sch); err != nil {
		return nil, err
	}
	rss.schemas = schemas

	for _, col := range columns {
		if err = rss.addColumn(col.Sch, col.Col); err != nil {
			return nil, err
		}
	}

	return rss, nil
}

// Adds a single column to the result set schema, from the source schema given.
func (rss *ResultSetSchema) addColumn(sourceSchema schema.Schema, column schema.Column) error {
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

// Concatenates the given schemas together into a new one. This rewrites the tag numbers to be contiguous and
// starting from zero, and removes all keys and constraints. Types are preserved.
func concatSchemas(srcSchemas ...schema.Schema) (schema.Schema, error) {
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

// CrossProductRowCallback is called once for each row produced by the CrossProduct call
type CrossProductRowCallback func(r row.Row)

// CrossProduct computes the cross-product of the table results given, calling the given callback once for each row in
// the result set. The resultant rows will have the schema of this result set, and will have (N * M * ... X) rows, one
// for every possible combination of entries in the table results given.
func (rss *ResultSetSchema) CrossProduct(nbf *types.NomsBinFormat, tables []*TableResult, cb CrossProductRowCallback) {
	// special case: no tables means no rows
	if len(tables) == 0 {
		return
	}

	emptyRow := RowWithSchema{row.New(nbf, rss.destSch, row.TaggedValues{}), rss.destSch}
	rss.cph(emptyRow, tables, cb)
}

// Recursive helper function for CrossProduct
func (rss *ResultSetSchema) cph(r RowWithSchema, tables []*TableResult, cb CrossProductRowCallback) {
	if len(tables) == 0 {
		cb(r.Row)
		return
	}

	table := tables[0]
	itr := table.Iterator()
	for r2 := itr.NextRow(); r2 != nil; r2 = itr.NextRow() {
		partialRow := rss.combineRows(r, RowWithSchema{r2, table.Schema})
		rss.cph(partialRow, tables[1:], cb)
	}
}

// CombineRows writes all values from r2 into r1 and returns it. r1 must have the same schema as the result set.
func (rss *ResultSetSchema) combineRows(r1 RowWithSchema, r2 RowWithSchema) RowWithSchema {
	if !schema.SchemasAreEqual(r1.Schema, rss.destSch) {
		panic("Cannot call CombineRows on a row with a different schema than the result set schema")
	}

	fieldMapping, ok := rss.mapping[r2.Schema]
	if !ok {
		panic(fmt.Sprintf("Unrecognized schema %v", r2.Schema))
	}

	r2.Row.IterCols(func(tag uint64, val types.Value) (stop bool) {
		mappedTag, ok := fieldMapping.SrcToDest[tag]
		if ok {
			if err := r1.SetColVal(mappedTag, val); err != nil {
				panic(err)
			}
		}
		return false
	})
	return r1
}

// CombineRows writes all values from other rows into r1 and returns it. r1 must have the same schema as the result set.
func (rss *ResultSetSchema) combineAllRows(r1 RowWithSchema, rows ...RowWithSchema) RowWithSchema {
	for _, r2 := range rows {
		r1 = rss.combineRows(r1, r2)
	}
	return r1
}
