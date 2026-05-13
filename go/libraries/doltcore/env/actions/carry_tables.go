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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// CarryUncommittedTables copies tables that are in |src| but not in |baseline| into |dest|,
// resolving column tag collisions, and carries forward the foreign key entries for those tables
// from |src| into |dest|'s FK collection. Tables already present in |dest| are kept unchanged.
// Callers choose |baseline| to control what counts as "untracked": pass the staged root for
// true untracked semantics or the head root to also include staged-but-uncommitted tables.
func CarryUncommittedTables(ctx context.Context, src, baseline, dest doltdb.RootValue) (doltdb.RootValue, error) {
	untracked, err := uncommittedSchemas(ctx, src, baseline)
	if err != nil {
		return nil, err
	}

	dest, carried, tagRemaps, err := carryTables(ctx, src, dest, untracked)
	if err != nil {
		return nil, err
	}

	if len(carried) == 0 {
		return dest, nil
	}

	return carryForeignKeys(ctx, src, dest, carried, tagRemaps)
}

// uncommittedSchemas returns the schemas of tables that are in |src| but not in |baseline|.
func uncommittedSchemas(ctx context.Context, src, baseline doltdb.RootValue) (map[doltdb.TableName]schema.Schema, error) {
	untracked, err := doltdb.GetAllSchemas(ctx, src)
	if err != nil {
		return nil, err
	}
	baselineNames, err := baseline.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}
	for _, name := range baselineNames {
		delete(untracked, name)
	}
	return untracked, nil
}

// carryTables copies each entry of |toCarry| from |src| into |dest|, retagging columns whose
// tag collides with one already in |dest| or with one assigned to an earlier carried table.
// Tables whose name already exists in |dest| are skipped so the dest version wins. Returns
// the updated |dest|, the names that were carried, and the tag remaps applied per table for
// downstream FK fixup.
func carryTables(ctx context.Context, src, dest doltdb.RootValue, toCarry map[doltdb.TableName]schema.Schema) (doltdb.RootValue, []doltdb.TableName, map[doltdb.TableName]map[uint64]uint64, error) {
	destSchemas, err := doltdb.GetAllSchemas(ctx, dest)
	if err != nil {
		return nil, nil, nil, err
	}

	heldTags := make(schema.TagMapping)
	for tblName, tblSch := range destSchemas {
		for _, t := range tblSch.GetAllCols().Tags {
			heldTags.Add(t, tblName.Name)
		}
	}

	allTagRemaps := make(map[doltdb.TableName]map[uint64]uint64)
	var carried []doltdb.TableName
	for name, sch := range toCarry {
		if _, exists := destSchemas[name]; exists {
			continue
		}
		tbl, exists, err := src.GetTable(ctx, name)
		if err != nil {
			return nil, nil, nil, err
		}
		if !exists {
			return nil, nil, nil, fmt.Errorf("table %s does not exist in src root", name)
		}
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
			return nil, nil, nil, fmt.Errorf("failed to write table back to database: %w", err)
		}
		carried = append(carried, name)
	}
	return dest, carried, allTagRemaps, nil
}

// carryForeignKeys copies foreign keys from |src| whose child table is in |carried| into
// |dest|'s FK collection, applies |tagRemaps| so retagged column references are updated, and
// returns the updated |dest|. Foreign keys already present in |dest| (e.g. pre-merged by an
// upstream step) are skipped to avoid ErrForeignKeyDuplicateName. When a carried foreign key
// references a parent table that already exists on |dest|, the referenced column tags are
// re-resolved against |dest|'s parent schema by column name so the foreign key remains valid
// even when the same parent column has a different internal tag on |src| and |dest|.
func carryForeignKeys(ctx context.Context, src, dest doltdb.RootValue, carried []doltdb.TableName, tagRemaps map[doltdb.TableName]map[uint64]uint64) (doltdb.RootValue, error) {
	destFks, err := dest.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	srcFks, err := src.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	srcSchs, err := doltdb.GetAllSchemas(ctx, src)
	if err != nil {
		return nil, err
	}
	destSchs, err := doltdb.GetAllSchemas(ctx, dest)
	if err != nil {
		return nil, err
	}
	carriedSet := doltdb.NewTableNameSet(carried)
	err = srcFks.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		if !carriedSet.Contains(fk.TableName) {
			return false, nil
		}
		if _, exists := destFks.GetByNameCaseInsensitive(fk.Name, fk.TableName); exists {
			return false, nil
		}
		fk.TableColumns = schema.RemapTags(fk.TableColumns, tagRemaps[fk.TableName])
		if carriedSet.Contains(fk.ReferencedTableName) {
			fk.ReferencedTableColumns = schema.RemapTags(fk.ReferencedTableColumns, tagRemaps[fk.ReferencedTableName])
		} else {
			fk.ReferencedTableColumns = remapTagsByColumnName(fk.ReferencedTableColumns, srcSchs[fk.ReferencedTableName], destSchs[fk.ReferencedTableName])
		}
		return false, destFks.AddKeys(fk)
	})
	if err != nil {
		return nil, err
	}
	return dest.PutForeignKeyCollection(ctx, destFks)
}

// remapTagsByColumnName returns |tags| rewritten so each tag points at the same-named column
// on |destSch| instead of |srcSch|. When either schema is nil or any tag cannot be
// resolved on both sides, the original |tags| are returned so the caller can proceed and let
// downstream validation surface any genuine breakage.
func remapTagsByColumnName(tags []uint64, srcSch, destSch schema.Schema) []uint64 {
	if srcSch == nil || destSch == nil {
		return tags
	}
	out := make([]uint64, len(tags))
	for i, srcTag := range tags {
		srcCol, ok := srcSch.GetAllCols().GetByTag(srcTag)
		if !ok {
			return tags
		}
		destCol, ok := destSch.GetAllCols().GetByName(srcCol.Name)
		if !ok {
			return tags
		}
		out[i] = destCol.Tag
	}
	return out
}
