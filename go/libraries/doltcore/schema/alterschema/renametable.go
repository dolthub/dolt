package alterschema

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
)

// RenameTable renames a table with in a RootValue and returns the updated root.
func RenameTable(ctx context.Context, doltDb *doltdb.DoltDB, root *doltdb.RootValue, oldName, newName string) (*doltdb.RootValue, error) {
	if newName == oldName {
		return root, nil
	} else if root == nil {
		panic("invalid parameters")
	}

	tbl, ok := root.GetTable(ctx, oldName)
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	if root.HasTable(ctx, newName) {
		return nil, doltdb.ErrTableExists
	}

	var err error
	if root, err = root.RemoveTables(ctx, oldName); err != nil {
		return nil, err
	}

	return root.PutTable(ctx, doltDb, newName, tbl), nil
}
