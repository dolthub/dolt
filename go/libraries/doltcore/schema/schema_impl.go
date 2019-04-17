package schema

var EmptySchema = &schemaImpl{
	emptyColColl,
	emptyColColl,
	emptyColColl,
}

type schemaImpl struct {
	pkCols, nonPKCols, allCols *ColCollection
}

func SchemaFromColMap(allCols map[string]Column) (Schema, error) {
	colColl, err := NewColCollectionFromMap(allCols)

	if err != nil {
		return EmptySchema, err
	}

	return SchemaFromCols(colColl), nil
}

// SchemaFromCols creates a Schema from a collection of columns
func SchemaFromCols(allCols *ColCollection) Schema {
	var pkCols []Column
	var nonPKCols []Column

	for _, c := range allCols.cols {
		if c.IsPartOfPK {
			pkCols = append(pkCols, c)
		} else {
			nonPKCols = append(nonPKCols, c)
		}
	}

	if len(pkCols) == 0 {
		panic("no primary key columns specified")
	}

	pkColColl, _ := NewColCollection(pkCols...)
	nonPKColColl, _ := NewColCollection(nonPKCols...)

	return &schemaImpl{
		pkColColl, nonPKColColl, allCols,
	}
}

// UnkeyedSchemaFromCols creates a schema without any primary keys to be used for displaying to users, tests, etc. Such
// unkeyed schemas are not suitable to be inserted into storage.
func UnkeyedSchemaFromCols(allCols *ColCollection) Schema {
	var nonPKCols []Column

	for _, c := range allCols.cols {
		c.IsPartOfPK = false
		nonPKCols = append(nonPKCols, c)
	}

	pkColColl, _ := NewColCollection()
	nonPKColColl, _ := NewColCollection(nonPKCols...)

	return &schemaImpl{
		pkColColl, nonPKColColl, allCols,
	}
}

// SchemaFromPKAndNonPKCols creates a Schema from a collection of the key columns, and the non-key columns.
func SchemaFromPKAndNonPKCols(pkCols, nonPKCols *ColCollection) (Schema, error) {
	allCols := make([]Column, pkCols.Size()+nonPKCols.Size())

	i := 0
	for _, c := range pkCols.cols {
		if !c.IsPartOfPK {
			panic("bug: attempting to add a column to the pk that isn't part of the pk")
		}

		allCols[i] = c
		i++
	}

	for _, c := range nonPKCols.cols {
		if c.IsPartOfPK {
			panic("bug: attempting to add a column that is part of the pk to the non-pk columns")
		}

		allCols[i] = c
		i++
	}

	allColColl, err := NewColCollection(allCols...)

	if err != nil {
		return nil, err
	}

	return &schemaImpl{
		pkCols, nonPKCols, allColColl,
	}, nil
}

// GetAllCols gets the collection of all columns (pk and non-pk)
func (si *schemaImpl) GetAllCols() *ColCollection {
	return si.allCols
}

// GetNonPKCols gets the collection of columns which are not part of the primary key.
func (si *schemaImpl) GetNonPKCols() *ColCollection {
	return si.nonPKCols
}

// GetPKCols gets the collection of columns which make the primary key.
func (si *schemaImpl) GetPKCols() *ColCollection {
	return si.pkCols
}
