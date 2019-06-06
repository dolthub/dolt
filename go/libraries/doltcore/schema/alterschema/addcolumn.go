package alterschema

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
)

type Nullable bool
const(
	NotNull Nullable = false
	Null Nullable = true
)

// Adds a new column to the schema given and returns the new table value. Non-null column additions rewrite the entire
// table, since we must write a value for each row. If the column is not nullable, a default value must be provided.
//
// Returns an error if the column added conflicts with the existing schema in tag or name.
func AddColumnToTable(ctx context.Context, db *doltdb.DoltDB, tbl *doltdb.Table, tag uint64, newColName string, colKind types.NomsKind, nullable Nullable, defaultVal types.Value) (*doltdb.Table, error) {
	tblSch := tbl.GetSchema(ctx)

	var err error
	cols := tblSch.GetAllCols()
	cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool) {
		if currColTag == tag {
			err = errhand.BuildDError("A column with the tag %d already exists.", tag).Build()
			return true
		} else if currCol.Name == newColName {
			err = errhand.BuildDError("A column with the name %s already exists.", newColName).Build()
			return true
		}

		return false
	})

	var col schema.Column
	if nullable {
		col = schema.NewColumn(newColName, tag, colKind, false)
	} else {
		col = schema.NewColumn(newColName, tag, colKind, false, schema.NotNullConstraint{})
	}

	sch := tbl.GetSchema(ctx)
	updatedCols, err := sch.GetAllCols().Append(col)
	if err != nil {
		return nil, err
	}

	vrw := db.ValueReadWriter()
	newSchema := schema.SchemaFromCols(updatedCols)
	newSchemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, newSchema)
	if err != nil {
		return nil, err
	}

	rowData := tbl.GetRowData(ctx)
	if !nullable && defaultVal == nil && rowData.Len() > 0 {
		return nil, errhand.BuildDError("When adding a column that may not be null to a table with existing " +
			"rows, a default value must be provided.").Build()
	}

	if defaultVal == nil {
		newTable := doltdb.NewTable(ctx, vrw, newSchemaVal, rowData)
		return newTable, nil
	} else if defaultVal.Kind() != colKind {
		return nil, errhand.BuildDError("Type of default value (%v) doesn't match type of column (%v)", types.KindToString[defaultVal.Kind()], types.KindToString[colKind]).Build()
	}

	me := rowData.Edit()

	var updateErr error
	rowData.Iter(ctx, func(k, v types.Value) (stop bool) {
		oldRow, _ := tbl.GetRow(ctx, k.(types.Tuple), newSchema)
		newRow, err := oldRow.SetColVal(tag, defaultVal, newSchema)
		if err != nil {
			updateErr = err
			return true
		}

		me.Set(newRow.NomsMapKey(newSchema), newRow.NomsMapValue(newSchema))
		return false
	})
	if updateErr != nil {
		return nil, updateErr
	}

	updatedTbl := doltdb.NewTable(ctx, vrw, newSchemaVal, me.Map(ctx))
	return updatedTbl, nil
}

