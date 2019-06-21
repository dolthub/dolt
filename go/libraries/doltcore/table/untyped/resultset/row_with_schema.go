package resultset

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/go/types"
)

// For most analysis, rows can be considered independent of their schema (schemas just provide type / tag number
// validation). This falls apart when we need to combine rows from multiple tables together into a new result set that
// is some combination of the source schemas. For these use cases, it becomes very useful to package row info with the
// schema of the table it came from.
type RowWithSchema struct {
	Row    row.Row
	Schema schema.Schema
}

// Updates the column value. Unlike Row, RowWithSchema is mutable. Calling this method updates the underlying row.
func (r *RowWithSchema) SetColVal(tag uint64, value types.Value) error {
	newRow, err := r.Row.SetColVal(tag, value, r.Schema)
	if err != nil {
		return err
	}
	r.Row = newRow
	return nil
}

// Returns the underlying column value for the tag given. Convenience method for calling GetColVal on the underlying
// row field.
func (r *RowWithSchema) GetColVal(tag uint64) (types.Value, bool) {
	return r.Row.GetColVal(tag)
}

// Returns a copy of this row.
func (r *RowWithSchema) Copy() RowWithSchema {
	return RowWithSchema{r.Row, r.Schema}
}
