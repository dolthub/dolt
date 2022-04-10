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

func CheckoutAllTables(ctx context.Context, roots doltdb.Roots, dbData env.DbData) error {
	tbls, err := doltdb.UnionTableNames(ctx, roots.Working, roots.Staged, roots.Head)
	if err != nil {
		return err
	}

	docs := doltdocs.SupportedDocs
	return checkoutTablesAndDocs(ctx, dbData, roots, tbls, docs)
}

// CheckoutTablesAndDocs takes in a set of tables and docs and checks them out to another branch.
func CheckoutTablesAndDocs(ctx context.Context, roots doltdb.Roots, dbData env.DbData, tables []string, docs doltdocs.Docs) error {
	return checkoutTablesAndDocs(ctx, dbData, roots, tables, docs)
}

func checkoutTables(ctx context.Context, dbData env.DbData, roots doltdb.Roots, tbls []string) (doltdb.Roots, error) {
	roots, err := MoveTablesFromHeadToWorking(ctx, roots, tbls)
	if err != nil {
		return doltdb.Roots{}, err
	}

	err = dbData.Rsw.UpdateWorkingRoot(ctx, roots.Working)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return roots, nil
}

// MoveTablesFromHeadToWorking replaces the tables named from the given head to the given working root, overwriting any
// working changes, and returns the new resulting roots
func MoveTablesFromHeadToWorking(ctx context.Context, roots doltdb.Roots, tbls []string) (doltdb.Roots, error) {
	var unknownTbls []string
	for _, tblName := range tbls {
		// TODO: not at all clear why this should be excluded (this code was moved from elsewhere)
		if tblName == doltdb.DocTableName {
			continue
		}
		tbl, ok, err := roots.Staged.GetTable(ctx, tblName)

		if err != nil {
			return doltdb.Roots{}, err
		}

		if !ok {
			tbl, ok, err = roots.Head.GetTable(ctx, tblName)

			if err != nil {
				return doltdb.Roots{}, err
			}

			if !ok {
				unknownTbls = append(unknownTbls, tblName)
				continue
			}
		}

		roots.Working, err = roots.Working.PutTable(ctx, tblName, tbl)

		if err != nil {
			return doltdb.Roots{}, err
		}
	}

	if len(unknownTbls) > 0 {
		// Return table not exist error before RemoveTables, which fails silently if the table is not on the root.
		err := validateTablesExist(ctx, roots.Working, unknownTbls)
		if err != nil {
			return doltdb.Roots{}, err
		}

		roots.Working, err = roots.Working.RemoveTables(ctx, false, false, unknownTbls...)

		if err != nil {
			return doltdb.Roots{}, err
		}
	}

	return roots, nil
}

func checkoutDocs(ctx context.Context, dbData env.DbData, roots doltdb.Roots, docs doltdocs.Docs) error {
	if len(docs) > 0 {
		var err error
		roots, docs, err = getUpdatedWorkingAndStagedWithDocs(ctx, roots, docs)
		if err != nil {
			return err
		}
	}

	err := dbData.Rsw.UpdateWorkingRoot(ctx, roots.Working)
	if err != nil {
		return err
	}

	return dbData.Drw.WriteDocsToDisk(docs)
}

func checkoutTablesAndDocs(ctx context.Context, dbData env.DbData, roots doltdb.Roots, tbls []string, docs doltdocs.Docs) error {
	var err error
	roots, err = checkoutTables(ctx, dbData, roots, tbls)
	if err != nil {
		return err
	}

	return checkoutDocs(ctx, dbData, roots, docs)
}
