package alterschema

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
)

// RenameColumn takes a table and renames a column from oldName to newName
func RenameColumn(ctx context.Context, doltDB *doltdb.DoltDB, tbl *doltdb.Table, oldName, newName string) (*doltdb.Table, error) {
	if newName == oldName {
		return tbl, nil
	} else if tbl == nil || doltDB == nil {
		panic("invalid parameters")
	}

	tblSch := tbl.GetSchema(ctx)
	allCols := tblSch.GetAllCols()

	if _, ok := allCols.GetByName(newName); ok {
		return nil, schema.ErrColNameCollision
	}

	if _, ok := allCols.GetByName(oldName); !ok {
		return nil, schema.ErrColNotFound
	}

	cols := make([]schema.Column, 0)
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Name == oldName {
			col.Name = newName
		}
		cols = append(cols, col)
		return false
	})

	colColl, err := schema.NewColCollection(cols...)
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
