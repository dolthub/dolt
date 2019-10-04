// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actions

import (
	"context"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

func CheckoutAllTables(ctx context.Context, dEnv *env.DoltEnv) error {
	roots, err := getRoots(ctx, dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	tbls, err := AllTables(ctx, roots[WorkingRoot], roots[StagedRoot], roots[HeadRoot])

	if err != nil {
		return err
	}

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
		tbl, ok, err := staged.GetTable(ctx, tblName)

		if err != nil {
			return err
		}

		if !ok {
			tbl, ok, err = head.GetTable(ctx, tblName)

			if err != nil {
				return err
			}

			if !ok {
				unknown = append(unknown, tblName)
				continue
			}
		}

		currRoot, err = currRoot.PutTable(ctx, tblName, tbl)

		if err != nil {
			return err
		}
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
