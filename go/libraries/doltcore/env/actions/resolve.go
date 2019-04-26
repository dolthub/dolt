package actions

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/merge"
)

type AutoResolveStats struct {
}

func AutoResolveAll(ctx context.Context, dEnv *env.DoltEnv, autoResolver merge.AutoResolver) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return err
	}

	tbls := root.TablesInConflict(ctx)
	return autoResolve(ctx, dEnv, root, autoResolver, tbls)
}

func AutoResolveTables(ctx context.Context, dEnv *env.DoltEnv, autoResolver merge.AutoResolver, tbls []string) error {
	root, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return err
	}

	return autoResolve(ctx, dEnv, root, autoResolver, tbls)
}

func autoResolve(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, autoResolver merge.AutoResolver, tbls []string) error {
	for _, tblName := range tbls {
		tbl, ok := root.GetTable(ctx, tblName)

		if !ok {
			return doltdb.ErrTableNotFound
		}

		updatedTbl, err := merge.ResolveTable(ctx, root.VRW(), tbl, autoResolver)

		if err != nil {
			return err
		}

		root = root.PutTable(ctx, dEnv.DoltDB, tblName, updatedTbl)
	}

	return dEnv.UpdateWorkingRoot(ctx, root)
}
