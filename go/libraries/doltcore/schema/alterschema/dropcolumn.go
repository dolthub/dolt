package alterschema

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
)

// DropColumn drops a column from a table. No existing rows are modified, but a new schema entry is written.
func DropColumn(ctx context.Context, doltDB *doltdb.DoltDB, tbl *doltdb.Table, colName string) (*doltdb.Table, error) {
	if tbl == nil || doltDB == nil {
		panic("invalid parameters")
	}

	tblSch := tbl.GetSchema(ctx)
	allCols := tblSch.GetAllCols()
	_, ok := allCols.GetByName(colName)

	if !ok {
		return nil, schema.ErrColNotFound
	}

	colMap := allCols.NameToCol
	delete(colMap, colName)

	colColl, err := schema.NewColCollectionFromMap(colMap)

	if err != nil {
		return nil, err
	}

	newSch := schema.SchemaFromCols(colColl)

	vrw := doltDB.ValueReadWriter()
	schemaVal, err := encoding.MarshalAsNomsValue(ctx, vrw, newSch)

	if err != nil {
		return nil, err
	}

	newTable := doltdb.NewTable(ctx, vrw, schemaVal, tbl.GetRowData(ctx))

	return newTable, nil
}
