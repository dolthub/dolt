package nullprinter

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

const PRINTED_NULL = "<NULL>"

const NULL_PRINTING_STAGE = "null printing"

// NullPrinter is a utility to convert nil values in rows to a string representation.
type NullPrinter struct {
	Sch schema.Schema
}

// NewNullPrinter returns a new null printer for the schema given, which must be string-typed (untyped).
func NewNullPrinter(sch schema.Schema) *NullPrinter {
	return &NullPrinter{Sch: sch}
}

// Function to convert any nil values for a row with the schema given to a string representation. Used as the transform
// function in a NamedTransform.
func (np *NullPrinter) ProcessRow(inRow row.Row, props pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
	taggedVals := make(row.TaggedValues)

	inRow.IterSchema(np.Sch, func(tag uint64, val types.Value) (stop bool) {
		if !types.IsNull(val) {
			taggedVals[tag] = val
		} else {
			taggedVals[tag] = types.String(PRINTED_NULL)
		}

		return false
	})

	return []*pipeline.TransformedRowResult{{RowData: row.New(np.Sch, taggedVals)}}, ""
}
