package typed

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
)

func TypedSchemaUnion(schemas ...schema.Schema) (schema.Schema, error) {
	var allCols []schema.Column

	for _, sch := range schemas {
		sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
			allCols = append(allCols, col)
			return false
		})
	}

	allColColl, err := schema.NewColCollection(allCols...)

	if err != nil {
		return nil, err
	}

	sch := schema.SchemaFromCols(allColColl)
	return sch, nil
}
