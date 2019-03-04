package schema

type schemaImpl struct {
	pkCols, nonPKCols, allCols *ColCollection
}

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

	pkColColl, _ := NewColCollection(pkCols...)
	nonPKColColl, _ := NewColCollection(nonPKCols...)

	si := &schemaImpl{
		pkColColl, nonPKColColl, allCols,
	}

	return si
}

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

func (si *schemaImpl) GetAllCols() *ColCollection {
	return si.allCols
}

func (si *schemaImpl) GetNonPKCols() *ColCollection {
	return si.nonPKCols
}

func (si *schemaImpl) GetPKCols() *ColCollection {
	return si.pkCols
}
