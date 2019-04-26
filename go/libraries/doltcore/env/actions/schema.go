package actions

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
)

// RenameColumnOfSchema takes a table and renames a column from oldName to newName
func RenameColumnOfSchema(ctx context.Context, oldName string, newName string, tbl *doltdb.Table, doltDB *doltdb.DoltDB) (*doltdb.Table, error) {
	if newName == oldName {
		return tbl, nil
	} else if tbl == nil || doltDB == nil {
		panic("invalid parameters")
	}

	tblSch := tbl.GetSchema()
	allCols := tblSch.GetAllCols()
	col, ok := allCols.GetByName(oldName)

	if !ok {
		return nil, schema.ErrColNotFound
	}

	_, ok = allCols.GetByName(newName)

	if ok {
		return nil, schema.ErrColNameCollision
	}

	col.Name = newName
	colMap := allCols.NameToCol
	colMap[newName] = col
	delete(colMap, oldName)

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

	newTable := doltdb.NewTable(ctx, vrw, schemaVal, tbl.GetRowData())

	return newTable, nil
}

// RemoveColumnFromTable takes a table and removes a column
func RemoveColumnFromTable(ctx context.Context, colName string, tbl *doltdb.Table, doltDB *doltdb.DoltDB) (*doltdb.Table, error) {
	if tbl == nil || doltDB == nil {
		panic("invalid parameters")
	}

	tblSch := tbl.GetSchema()
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

	newTable := doltdb.NewTable(ctx, vrw, schemaVal, tbl.GetRowData())

	return newTable, nil
}
