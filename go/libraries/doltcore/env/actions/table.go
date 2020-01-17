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

	docs := *env.AllValidDocDetails

	return checkoutTablesAndDocs(ctx, dEnv, roots, tbls, docs)

}

func CheckoutTablesAndDocs(ctx context.Context, dEnv *env.DoltEnv, tbls []string, docs []doltdb.DocDetails) error {
	roots, err := getRoots(ctx, dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	return checkoutTablesAndDocs(ctx, dEnv, roots, tbls, docs)
}

func checkoutTablesAndDocs(ctx context.Context, dEnv *env.DoltEnv, roots map[RootType]*doltdb.RootValue, tbls []string, docs []doltdb.DocDetails) error {
	unknownTbls := []string{}

	currRoot := roots[WorkingRoot]
	staged := roots[StagedRoot]
	head := roots[HeadRoot]

	if len(docs) > 0 {
		currRootWithDocs, stagedWithDocs, err := getUpdatedWorkingAndStagedWithDocs(ctx, dEnv, currRoot, staged, head, docs)
		if err != nil {
			return err
		}
		currRoot = currRootWithDocs
		staged = stagedWithDocs
	}

	for _, tblName := range tbls {
		if tblName == doltdb.DocTableName {
			continue
		}
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
				unknownTbls = append(unknownTbls, tblName)
				continue
			}
		}

		currRoot, err = currRoot.PutTable(ctx, tblName, tbl)

		if err != nil {
			return err
		}
	}

	if len(unknownTbls) > 0 {
		// Return table not exist error before RemoveTables, which fails silently if the table is not on the root.
		err := getTblNotExistError(ctx, currRoot, unknownTbls)
		if err != nil {
			return err
		}

		currRoot, err = currRoot.RemoveTables(ctx, unknownTbls...)

		if err != nil {
			return err
		}
	}

	err := dEnv.UpdateWorkingRoot(ctx, currRoot)
	if err != nil {
		return err
	}

	return SaveDocsFromDocDetails(dEnv, docs)
}

func getTblNotExistError(ctx context.Context, currRoot *doltdb.RootValue, unknown []string) error {
	notExist := []string{}
	for _, tbl := range unknown {
		if has, err := currRoot.HasTable(ctx, tbl); err != nil {
			return err
		} else if !has {
			notExist = append(notExist, tbl)
		}
	}

	if len(notExist) > 0 {
		return NewTblNotExistError(notExist)
	}

	return nil
}
