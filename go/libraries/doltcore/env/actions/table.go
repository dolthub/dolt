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
	tbls, docs, err := GetTblsAndDocDetails(dEnv, tbls)
	if err != nil {
		return err
	}
	
	if len(docs) > 0 {
		root := head
		if _, ok, err := staged.GetTable(ctx, doltdb.DocTableName); err != nil {
			return err
		} else if ok {
			root = staged
		}
	
		currRoot, staged, err = checkoutDocsFromRoot(ctx, dEnv, root, docs)
		if err != nil {
			return err
		}
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

func checkoutDocsFromRoot(ctx context.Context, dEnv *env.DoltEnv, root *doltdb.RootValue, docs env.Docs) (workingRoot, stagedRoot *doltdb.RootValue, err error) {
	working, staged, err := getUpdatedWorkingAndStagedWithDocs(ctx, root, dEnv, docs)
	if err != nil {
		return nil, nil, err
	}
	err = dEnv.UpdateFSDocsToRootDocs(ctx, root, docs)
	if err != nil {
		return nil, nil, err
	}

	return working, staged, nil
}

func getUpdatedWorkingAndStagedWithDocs(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, docDetails env.Docs) (currRoot, stgRoot *doltdb.RootValue, err error) {
	docs, err := dEnv.GetDocsWithNewerTextFromRoot(ctx, root, docDetails)
	if err != nil {
		return nil, nil, err
	}

	working, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	staged, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	working, err = dEnv.GetUpdatedRootWithDocs(ctx, working, docs)
	if err != nil {
		return nil, nil, err
	}

	staged, err = dEnv.GetUpdatedRootWithDocs(ctx, staged, docs)
	if err != nil {
		return nil, nil, err
	}

	return working, staged, nil
}
