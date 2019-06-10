package alterschema

import (
	"context"
	"errors"
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

	if col, ok := allCols.GetByName(colName); !ok {
		return nil, schema.ErrColNotFound
	} else if col.IsPartOfPK {
		return nil, errors.New("Cannot drop column in primary key")
	}

	cols := make([]schema.Column, 0)
	allCols.Iter(func(tag uint64, col schema.Column) (stop bool) {
		if col.Name != colName {
			cols = append(cols, col)
		}
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
