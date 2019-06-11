package actions

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
)

func CheckoutAllTables(ctx context.Context, dEnv *env.DoltEnv) error {
	roots, err := getRoots(ctx, dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	tbls := AllTables(ctx, roots[WorkingRoot], roots[StagedRoot], roots[HeadRoot])
	return checkoutTables(ctx, dEnv, roots, tbls)

}

func CheckoutTables(ctx context.Context, dEnv *env.DoltEnv, tbls []string) error {
	roots, err := getRoots(ctx, dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	return checkoutTables(ctx, dEnv, roots, tbls)
}

func checkoutTables(ctx context.Context, dEnv *env.DoltEnv, roots map[RootType]*doltdb.RootValue, tbls []string) error {
	var unknown []string

	currRoot := roots[WorkingRoot]
	staged := roots[StagedRoot]
	head := roots[HeadRoot]
	for _, tblName := range tbls {
		tbl, ok := staged.GetTable(ctx, tblName)

		if !ok {
			tbl, ok = head.GetTable(ctx, tblName)

			if !ok {
				unknown = append(unknown, tblName)
				continue
			}
		}

		currRoot = currRoot.PutTable(ctx, dEnv.DoltDB, tblName, tbl)
	}

	if len(unknown) > 0 {
		var err error
		currRoot, err = currRoot.RemoveTables(ctx, unknown...)

		if err != nil {
			return err
		}
	}

	return dEnv.UpdateWorkingRoot(ctx, currRoot)
}
