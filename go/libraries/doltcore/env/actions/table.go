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

func CheckoutAllTables(ctx context.Context, dEnv *env.DoltEnv) (unknown string, err error) {
	roots, err := getRoots(ctx, dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return "", err
	}

	tbls, err := AllTables(ctx, roots[WorkingRoot], roots[StagedRoot], roots[HeadRoot])

	if err != nil {
		return "", err
	}

	docs := *env.AllValidDocDetails

	return checkoutTablesAndDocs(ctx, dEnv, roots, tbls, docs)

}

func CheckoutTablesAndDocs(ctx context.Context, dEnv *env.DoltEnv, tbls []string, docs []doltdb.DocDetails) (unknown string, err error) {
	roots, err := getRoots(ctx, dEnv, WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return "", err
	}

	return checkoutTablesAndDocs(ctx, dEnv, roots, tbls, docs)
}

func checkoutTablesAndDocs(ctx context.Context, dEnv *env.DoltEnv, roots map[RootType]*doltdb.RootValue, tbls []string, docs []doltdb.DocDetails) (unknown string, err error) {
	unknownTbls := []string{}

	currRoot := roots[WorkingRoot]
	staged := roots[StagedRoot]
	head := roots[HeadRoot]

	if len(docs) > 0 {
		root := head
		_, ok, err := staged.GetTable(ctx, doltdb.DocTableName)
		if err != nil {
			return "", err
		} else if ok {
			root = staged
		}

		currRoot, staged, err = getUpdatedWorkingAndStagedWithDocs(ctx, root, dEnv, currRoot, staged, docs)
		if err != nil {
			return "", err
		}
	}

	for _, tblName := range tbls {
		if tblName == doltdb.DocTableName {
			continue
		}
		tbl, ok, err := staged.GetTable(ctx, tblName)

		if err != nil {
			return "", err
		}

		if !ok {
			tbl, ok, err = head.GetTable(ctx, tblName)

			if err != nil {
				return "", err
			}

			if !ok {
				unknownTbls = append(unknownTbls, tblName)
				continue
			}
		}

		currRoot, err = currRoot.PutTable(ctx, tblName, tbl)

		if err != nil {
			return "", err
		}
	}

	if len(unknownTbls) > 0 {
		for _, tbl := range unknownTbls {
			if has, err := currRoot.HasTable(ctx, tbl); err != nil {
				return "", err
			} else if !has {
				return tbl, doltdb.ErrTableNotFound
			}
		}

		var err error
		currRoot, err = currRoot.RemoveTables(ctx, unknownTbls...)

		if err != nil {
			return "", err
		}
	}

	err = dEnv.UpdateWorkingRoot(ctx, currRoot)
	if err != nil {
		return "", err
	}

	return "", SaveDocsFromWorking(ctx, dEnv)
}

func getUpdatedWorkingAndStagedWithDocs(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, working *doltdb.RootValue, staged *doltdb.RootValue, docDetails []doltdb.DocDetails) (currRoot, stgRoot *doltdb.RootValue, err error) {
	docs, err := dEnv.GetDocsWithNewerTextFromRoot(ctx, root, docDetails)
	if err != nil {
		return nil, nil, err
	}

	currRoot, err = dEnv.GetUpdatedRootWithDocs(ctx, working, docs)
	if err != nil {
		return nil, nil, err
	}

	stgRoot, err = dEnv.GetUpdatedRootWithDocs(ctx, staged, docs)
	if err != nil {
		return nil, nil, err
	}

	return currRoot, stgRoot, nil
}
