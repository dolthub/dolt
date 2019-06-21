package alterschema

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// Nullable represents whether a column can have a null value.
type Nullable bool

const (
	NotNull Nullable = false
	Null    Nullable = true
)

// Adds a new column to the schema given and returns the new table value. Non-null column additions rewrite the entire
// table, since we must write a value for each row. If the column is not nullable, a default value must be provided.
//
// Returns an error if the column added conflicts with the existing schema in tag or name.
func AddColumnToTable(ctx context.Context, db *doltdb.DoltDB, tbl *doltdb.Table, tag uint64, newColName string, colKind types.NomsKind, nullable Nullable, defaultVal types.Value) (*doltdb.Table, error) {
	sch := tbl.GetSchema(ctx)

	if err := validateNewColumn(ctx, tbl, tag, newColName, colKind, nullable, defaultVal); err != nil {
		return nil, err
	}

	newSchema, err := createNewSchema(sch, tag, newColName, colKind, nullable)
	if err != nil {
		return nil, err
	}

	return updateTableWithNewSchema(ctx, db, tbl, tag, newSchema, defaultVal)
}

// updateTableWithNewSchema updates the existing table with a new schema and new values for the new column as necessary,
// and returns the new table.
func updateTableWithNewSchema(ctx context.Context, db *doltdb.DoltDB, tbl *doltdb.Table, tag uint64, newSchema schema.Schema, defaultVal types.Value) (*doltdb.Table, error) {
	vrw := db.ValueReadWriter()
	newSchemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, newSchema)
	if err != nil {
		return nil, err
	}

	rowData := tbl.GetRowData(ctx)
	if defaultVal == nil {
		newTable := doltdb.NewTable(ctx, vrw, newSchemaVal, rowData)
		return newTable, nil
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

	return doltdb.NewTable(ctx, vrw, newSchemaVal, me.Map(ctx)), nil
}

// createNewSchema Creates a new schema with a column as specified by the params.
func createNewSchema(sch schema.Schema, tag uint64, newColName string, colKind types.NomsKind, nullable Nullable) (schema.Schema, error) {
	var col schema.Column
	if nullable {
		col = schema.NewColumn(newColName, tag, colKind, false)
	} else {
		col = schema.NewColumn(newColName, tag, colKind, false, schema.NotNullConstraint{})
	}

	updatedCols, err := sch.GetAllCols().Append(col)
	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(updatedCols), nil
}

// validateNewColumn returns an error if the column as specified cannot be added to the schema given.
func validateNewColumn(ctx context.Context, tbl *doltdb.Table, tag uint64, newColName string, colKind types.NomsKind, nullable Nullable, defaultVal types.Value) error {
	var err error
	sch := tbl.GetSchema(ctx)
	cols := sch.GetAllCols()
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
	if err != nil {
		return err
	}

	if !nullable && defaultVal == nil && tbl.GetRowData(ctx).Len() > 0 {
		return errhand.BuildDError("When adding a column that may not be null to a table with existing " +
			"rows, a default value must be provided.").Build()
	}

	if !types.IsNull(defaultVal) && defaultVal.Kind() != colKind {
		return errhand.BuildDError("Type of default value (%v) doesn't match type of column (%v)", types.KindToString[defaultVal.Kind()], types.KindToString[colKind]).Build()
	}

	return nil
}
