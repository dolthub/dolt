// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

func CheckoutAllTables(ctx context.Context, dEnv *env.DoltEnv) error {
	roots, err := getRoots(ctx, dEnv.DoltDB, dEnv.RepoStateReader(), WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	tbls, err := doltdb.UnionTableNames(ctx, roots[WorkingRoot], roots[StagedRoot], roots[HeadRoot])

	if err != nil {
		return err
	}

	docs := *env.AllValidDocDetails

	return checkoutTablesAndDocs(ctx, dEnv, roots, tbls, docs)

}

func CheckoutTablesAndDocs(ctx context.Context, dEnv *env.DoltEnv, tbls []string, docs []doltdb.DocDetails) error {
	roots, err := getRoots(ctx, dEnv.DoltDB, dEnv.RepoStateReader(), WorkingRoot, StagedRoot, HeadRoot)

	if err != nil {
		return err
	}

	return checkoutTablesAndDocs(ctx, dEnv, roots, tbls, docs)
}

// MoveTablesBetweenRoots copies tables with names in tbls from the src RootValue to the dest RootValue.
// It matches tables between roots by column tags.
func MoveTablesBetweenRoots(ctx context.Context, tbls []string, src, dest *doltdb.RootValue) (*doltdb.RootValue, error) {
	tblSet := set.NewStrSet(tbls)

	stagedFKs, err := dest.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	tblDeltas, err := diff.GetTableDeltas(ctx, dest, src)
	if err != nil {
		return nil, err
	}

	tblsToDrop := set.NewStrSet(nil)

	for _, td := range tblDeltas {
		if td.IsDrop() {
			if !tblSet.Contains(td.FromName) {
				continue
			}

			tblsToDrop.Add(td.FromName)
		} else {
			if !tblSet.Contains(td.ToName) {
				continue
			}

			if td.IsRename() {
				// rename table before adding the new version so we don't have
				// two copies of the same table
				dest, err = dest.RenameTable(ctx, td.FromName, td.ToName)
				if err != nil {
					return nil, err
				}
			}

			dest, err = dest.PutTable(ctx, td.ToName, td.ToTable)
			if err != nil {
				return nil, err
			}

			stagedFKs.RemoveKeys(td.FromFks...)
			err = stagedFKs.AddKeys(td.ToFks...)
			if err != nil {
				return nil, err
			}

			ss, _, err := src.GetSuperSchema(ctx, td.ToName)
			if err != nil {
				return nil, err
			}

			dest, err = dest.PutSuperSchema(ctx, td.ToName, ss)
			if err != nil {
				return nil, err
			}
		}
	}

	dest, err = dest.PutForeignKeyCollection(ctx, stagedFKs)
	if err != nil {
		return nil, err
	}

	// RemoveTables also removes that table's ForeignKeys
	dest, err = dest.RemoveTables(ctx, tblsToDrop.AsSlice()...)
	if err != nil {
		return nil, err
	}

	return dest, nil
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
		err := validateTablesExist(ctx, currRoot, unknownTbls)
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

func validateTablesExist(ctx context.Context, currRoot *doltdb.RootValue, unknown []string) error {
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
