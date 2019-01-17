package typed

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/set"
)

func TypedSchemaUnion(schemas ...*schema.Schema) *schema.Schema {
	allCols := set.NewStrSet([]string{})
	var ordered []*schema.Field

	for _, sch := range schemas {
		if sch == nil {
			continue
		}

		for i := 0; i < sch.NumFields(); i++ {
			fld := sch.GetField(i)
			nameStr := fld.NameStr()

			if !allCols.Contains(nameStr) {
				allCols.Add(nameStr)
				ordered = append(ordered, fld)
			}
		}
	}

	return schema.NewSchema(ordered)
}
