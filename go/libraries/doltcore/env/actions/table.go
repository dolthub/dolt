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

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

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
			stagedFKs.RemoveKeys(td.FromFks...)
		}
	}
	for _, td := range tblDeltas {
		if !td.IsDrop() {
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
	dest, err = dest.RemoveTables(ctx, false, false, tblsToDrop.AsSlice()...)
	if err != nil {
		return nil, err
	}

	return dest, nil
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

// RemoveDocsTable takes a slice of table names and returns a new slice with DocTableName removed.
func RemoveDocsTable(tbls []string) []string {
	var result []string
	for _, tblName := range tbls {
		if tblName != doltdb.DocTableName {
			result = append(result, tblName)
		}
	}
	return result
}

// GetRemoteBranchRef returns the ref of a branch and ensures it matched with name. It will also return boolean value
// representing whether there is not match or not and an error if there is one.
func GetRemoteBranchRef(ctx context.Context, ddb *doltdb.DoltDB, name string) (ref.DoltRef, bool, error) {
	remoteRefFilter := map[ref.RefType]struct{}{ref.RemoteRefType: {}}
	refs, err := ddb.GetRefsOfType(ctx, remoteRefFilter)

	if err != nil {
		return nil, false, err
	}

	for _, rf := range refs {
		if remRef, ok := rf.(ref.RemoteRef); ok && remRef.GetBranch() == name {
			return rf, true, nil
		}
	}

	return nil, false, nil
}
