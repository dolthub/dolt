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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// MoveTablesBetweenRoots copies tables with names in tbls from the src RootValue to the dest RootValue.
// It matches tables between roots by column tags.
func MoveTablesBetweenRoots(ctx context.Context, tbls []doltdb.TableName, src, dest doltdb.RootValue) (doltdb.RootValue, error) {
	tablesToMove := doltdb.NewTableNameSet(tbls)

	stagedFKs, err := dest.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	tblDeltas, err := diff.GetTableDeltas(ctx, dest, src)
	if err != nil {
		return nil, err
	}

	addFullTextTablesToDelta(tblDeltas, tablesToMove)

	destSchemaNames, err := getDatabaseSchemaNames(ctx, dest)
	if err != nil {
		return nil, err
	}

	tblsToDrop := doltdb.NewTableNameSet(nil)
	for _, td := range tblDeltas {
		if td.IsDrop() {
			if !tablesToMove.Contains(td.FromName) {
				continue
			}

			stagedFKs.RemoveKeys(td.FromFks...)
			tblsToDrop.Add(td.FromName)
		}
	}

	for _, td := range tblDeltas {
		if !td.IsDrop() {
			if !tablesToMove.Contains(td.ToName) {
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

			// edge case: if we're moving a table with a schema name to a root that doesn't have that schema,
			// we implicitly create that schema on the destination root in addition to updating the list of schemas
			if td.ToName.Schema != "" && !destSchemaNames.Contains(td.ToName.Schema) {
				dest, err = dest.CreateDatabaseSchema(ctx, schema.DatabaseSchema{
					Name: td.ToName.Schema,
				})
				destSchemaNames.Add(td.ToName.Schema)
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

func getDatabaseSchemaNames(ctx context.Context, dest doltdb.RootValue) (*set.StrSet, error) {
	dbSchemaNames := set.NewEmptyStrSet()
	dbSchemas, err := dest.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, err
	}
	for _, dbSchema := range dbSchemas {
		dbSchemaNames.Add(dbSchema.Name)
	}
	return dbSchemaNames, nil
}

// addFullTextTablesToDelta adds the full text tables associated any full text indexes in the table deltas to the tableset provided
func addFullTextTablesToDelta(tblDeltas []diff.TableDelta, tblSet *doltdb.TableNameSet) {
	for _, td := range tblDeltas {
		var ftIndexes []schema.Index
		if tblSet.Contains(td.ToName) && td.ToSch.Indexes().ContainsFullTextIndex() {
			for _, idx := range td.ToSch.Indexes().AllIndexes() {
				if !idx.IsFullText() {
					continue
				}
				ftIndexes = append(ftIndexes, idx)
			}
		} else if tblSet.Contains(td.FromName) && td.FromSch.Indexes().ContainsFullTextIndex() {
			for _, idx := range td.FromSch.Indexes().AllIndexes() {
				if !idx.IsFullText() {
					continue
				}
				ftIndexes = append(ftIndexes, idx)
			}
		}
		for _, ftIndex := range ftIndexes {
			props := ftIndex.FullTextProperties()
			// TODO: schema names
			tblSet.Add(
				doltdb.TableName{Name: props.ConfigTable},
				doltdb.TableName{Name: props.PositionTable},
				doltdb.TableName{Name: props.DocCountTable},
				doltdb.TableName{Name: props.GlobalCountTable},
				doltdb.TableName{Name: props.RowCountTable},
			)
		}
	}
}

func validateTablesExist(ctx context.Context, currRoot doltdb.RootValue, unknown []doltdb.TableName) error {
	var notExist []doltdb.TableName
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
