// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

func CheckoutAllTables(ctx context.Context, dEnv *env.DoltEnv) error {
	roots, err := getRoots(ctx, dEnv.DoltDB, dEnv.RepoStateReader(), doltdb.WorkingRoot, doltdb.StagedRoot, doltdb.HeadRoot)

	if err != nil {
		return err
	}

	tbls, err := doltdb.UnionTableNames(ctx, roots[doltdb.WorkingRoot], roots[doltdb.StagedRoot], roots[doltdb.HeadRoot])

	if err != nil {
		return err
	}

	docs := doltdocs.SupportedDocs

	return checkoutTablesAndDocs(ctx, dEnv.DbData(), roots, tbls, docs)

}

func CheckoutTables(ctx context.Context, dbData env.DbData, tables []string) error {
	roots, err := getRoots(ctx, dbData.Ddb, dbData.Rsr, doltdb.WorkingRoot, doltdb.StagedRoot, doltdb.HeadRoot)

	if err != nil {
		return err
	}

	return checkoutTables(ctx, dbData, roots, tables)
}

// CheckoutTablesAndDocs takes in a set of tables and docs and checks them out to another branch.
func CheckoutTablesAndDocs(ctx context.Context, dbData env.DbData, tables []string, docs doltdocs.Docs) error {
	roots, err := getRoots(ctx, dbData.Ddb, dbData.Rsr, doltdb.WorkingRoot, doltdb.StagedRoot, doltdb.HeadRoot)

	if err != nil {
		return err
	}

	return checkoutTablesAndDocs(ctx, dbData, roots, tables, docs)
}

func checkoutTables(ctx context.Context, dbData env.DbData, roots map[doltdb.RootType]*doltdb.RootValue, tbls []string) error {
	unknownTbls := []string{}

	currRoot := roots[doltdb.WorkingRoot]
	staged := roots[doltdb.StagedRoot]
	head := roots[doltdb.HeadRoot]

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
		err := validateTablesExist(ctx, currRoot, unknownTbls)
		if err != nil {
			return err
		}

		currRoot, err = currRoot.RemoveTables(ctx, unknownTbls...)

		if err != nil {
			return err
		}
	}

	// update the working root with currRoot
	_, err := env.UpdateWorkingRoot(ctx, dbData.Ddb, dbData.Rsw, currRoot)

	return err
}

func checkoutDocs(ctx context.Context, dbData env.DbData, roots map[doltdb.RootType]*doltdb.RootValue, docs doltdocs.Docs) error {
	currRoot := roots[doltdb.WorkingRoot]
	staged := roots[doltdb.StagedRoot]
	head := roots[doltdb.HeadRoot]

	if len(docs) > 0 {
		currRootWithDocs, stagedWithDocs, updatedDocs, err := getUpdatedWorkingAndStagedWithDocs(ctx, currRoot, staged, head, docs)
		if err != nil {
			return err
		}
		currRoot = currRootWithDocs
		staged = stagedWithDocs
		docs = updatedDocs
	}

	_, err := env.UpdateWorkingRoot(ctx, dbData.Ddb, dbData.Rsw, currRoot)
	if err != nil {
		return err
	}

	return dbData.Drw.WriteDocsToDisk(docs)
}

func checkoutTablesAndDocs(ctx context.Context, dbData env.DbData, roots map[doltdb.RootType]*doltdb.RootValue, tbls []string, docs doltdocs.Docs) error {
	err := checkoutTables(ctx, dbData, roots, tbls)

	if err != nil {
		return err
	}

	roots, err = getRoots(ctx, dbData.Ddb, dbData.Rsr, doltdb.WorkingRoot, doltdb.StagedRoot, doltdb.HeadRoot)

	if err != nil {
		return err
	}

	return checkoutDocs(ctx, dbData, roots, docs)
}
