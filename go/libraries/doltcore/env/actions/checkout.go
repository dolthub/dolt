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
)

func CheckoutAllTables(ctx context.Context, roots doltdb.Roots) (doltdb.Roots, error) {
	tbls, err := doltdb.UnionTableNames(ctx, roots.Working, roots.Staged, roots.Head)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return MoveTablesFromHeadToWorking(ctx, roots, tbls)
}

// CheckoutTables takes in a set of tables and docs and checks them out to another branch.
func CheckoutTables(ctx context.Context, roots doltdb.Roots, tables []string) (doltdb.Roots, error) {
	return MoveTablesFromHeadToWorking(ctx, roots, tables)
}

// MoveTablesFromHeadToWorking replaces the tables named from the given head to the given working root, overwriting any
// working changes, and returns the new resulting roots
func MoveTablesFromHeadToWorking(ctx context.Context, roots doltdb.Roots, tbls []string) (doltdb.Roots, error) {
	var unknownTbls []string
	for _, tblName := range tbls {
		tbl, ok, err := roots.Staged.GetTable(ctx, tblName)
		if err != nil {
			return doltdb.Roots{}, err
		}
		fkc, err := roots.Staged.GetForeignKeyCollection(ctx)
		if err != nil {
			return doltdb.Roots{}, err
		}

		if !ok {
			tbl, ok, err = roots.Head.GetTable(ctx, tblName)
			if err != nil {
				return doltdb.Roots{}, err
			}

			fkc, err = roots.Head.GetForeignKeyCollection(ctx)
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

		roots.Working, err = roots.Working.PutForeignKeyCollection(ctx, fkc)
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
