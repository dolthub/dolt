// Copyright 2026 Dolthub, Inc.
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
	"fmt"
	"slices"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// CarryTablesAbsentFromBaseline copies tables that exist in |src| but not in |baseline| into
// |dest|, resolving column tag collisions and carrying matching foreign keys. Tables already
// on |dest| are left alone. Names in |exclude| are skipped; pass nil to carry all.
func CarryTablesAbsentFromBaseline(ctx context.Context, src, baseline, dest doltdb.RootValue, exclude *doltdb.TableNameSet) (doltdb.RootValue, error) {
	srcSchemas, err := doltdb.GetAllSchemas(ctx, src)
	if err != nil {
		return nil, err
	}
	destSchemas, err := doltdb.GetAllSchemas(ctx, dest)
	if err != nil {
		return nil, err
	}

	absent, err := tablesAbsentFromBaseline(ctx, srcSchemas, baseline, exclude)
	if err != nil {
		return nil, err
	}

	dest, carried, tagRemaps, err := carryTables(ctx, src, dest, absent, destSchemas)
	if err != nil {
		return nil, err
	}

	if len(carried) == 0 {
		return dest, nil
	}

	return carryForeignKeys(ctx, src, dest, carried, tagRemaps, srcSchemas, destSchemas)
}

// tablesAbsentFromBaseline returns the entries of |srcSchemas| not present in |baseline|,
// excluding read-only dolt system tables and any names in |exclude|.
func tablesAbsentFromBaseline(ctx context.Context, srcSchemas map[doltdb.TableName]schema.Schema, baseline doltdb.RootValue, exclude *doltdb.TableNameSet) (map[doltdb.TableName]schema.Schema, error) {
	baselineNames, err := baseline.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}
	baselineSet := doltdb.NewTableNameSet(baselineNames)

	absent := make(map[doltdb.TableName]schema.Schema, len(srcSchemas))
	for name, sch := range srcSchemas {
		if baselineSet.Contains(name) || doltdb.IsReadOnlySystemTable(name) {
			continue
		}
		if exclude != nil && exclude.Contains(name) {
			continue
		}
		absent[name] = sch
	}
	return absent, nil
}

// carryTables copies each entry of |toCarry| from |src| into |dest|, retagging columns whose
// tag collides with one held by |dest| or by an earlier carried table. Names already on |dest|
// are skipped so the dest version wins. Returns the updated |dest|, the names that were
// carried, and the per-table tag remaps so the caller can fix up foreign keys.
func carryTables(ctx context.Context, src, dest doltdb.RootValue, toCarry, destSchemas map[doltdb.TableName]schema.Schema) (doltdb.RootValue, []doltdb.TableName, map[doltdb.TableName]map[uint64]uint64, error) {
	heldTags := make(schema.TagMapping)
	for tblName, tblSch := range destSchemas {
		for _, t := range tblSch.GetAllCols().Tags {
			heldTags.Add(t, tblName.Name)
		}
	}

	allTagRemaps := make(map[doltdb.TableName]map[uint64]uint64)
	var carried []doltdb.TableName
	// Sort so retags reproduce across runs. Each table's new tags depend on tables
	// carried before it.
	names := make([]doltdb.TableName, 0, len(toCarry))
	for name := range toCarry {
		names = append(names, name)
	}
	slices.SortFunc(names, func(a, b doltdb.TableName) int { return strings.Compare(a.String(), b.String()) })
	for _, name := range names {
		if _, exists := destSchemas[name]; exists {
			continue
		}

		tbl, exists, err := src.GetTable(ctx, name)
		if err != nil {
			return nil, nil, nil, err
		}
		if !exists {
			return nil, nil, nil, fmt.Errorf("table %q does not exist in src root", name.String())
		}

		sch := toCarry[name]
		columns := sch.GetAllCols().GetColumns()
		tagRemap := make(map[uint64]uint64)
		priorKinds := make([]types.NomsKind, 0, len(columns))
		for _, column := range columns {
			if heldTags.Contains(column.Tag) {
				newTag := schema.AutoGenerateTag(heldTags, name.Name, priorKinds, column.Name, column.Kind)
				tagRemap[column.Tag] = newTag
				column.Tag = newTag
			}
			priorKinds = append(priorKinds, column.Kind)
			heldTags.Add(column.Tag, name.Name)
		}

		if len(tagRemap) > 0 {
			allTagRemaps[name] = tagRemap
			newSch, err := schema.WithRemappedColumnTags(sch, tagRemap)
			if err != nil {
				return nil, nil, nil, err
			}
			tbl, err = tbl.UpdateSchema(ctx, newSch)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		dest, err = dest.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, nil, nil, err
		}
		carried = append(carried, name)
	}
	return dest, carried, allTagRemaps, nil
}

// carryForeignKeys copies foreign keys from |src| whose child table is in |carried| into
// |dest|, applying |tagRemaps| to child columns. Parent column tags are re-resolved by
// column name via |destSchemas| when the parent already lives on |dest|. An existing key
// of the same name on |dest| is replaced.
func carryForeignKeys(ctx context.Context, src, dest doltdb.RootValue, carried []doltdb.TableName, tagRemaps map[doltdb.TableName]map[uint64]uint64, srcSchemas, destSchemas map[doltdb.TableName]schema.Schema) (doltdb.RootValue, error) {
	destFks, err := dest.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	srcFks, err := src.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	carriedSet := doltdb.NewTableNameSet(carried)
	err = srcFks.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		if !carriedSet.Contains(fk.TableName) {
			return false, nil
		}
		fk.TableColumns = schema.RemapTags(fk.TableColumns, tagRemaps[fk.TableName])
		if carriedSet.Contains(fk.ReferencedTableName) {
			fk.ReferencedTableColumns = schema.RemapTags(fk.ReferencedTableColumns, tagRemaps[fk.ReferencedTableName])
		} else {
			fk.ReferencedTableColumns = schema.RemapTagsByColumnName(fk.ReferencedTableColumns, srcSchemas[fk.ReferencedTableName], destSchemas[fk.ReferencedTableName])
		}
		// Remove the merge's stale copy first. AddKeys would otherwise refuse the duplicate name.
		destFks.RemoveKeyByName(fk.Name, fk.TableName)
		return false, destFks.AddKeys(fk)
	})
	if err != nil {
		return nil, err
	}
	return dest.PutForeignKeyCollection(ctx, destFks)
}
