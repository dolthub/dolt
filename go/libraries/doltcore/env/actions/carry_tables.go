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

// CarryPolicy selects which uncommitted tables CarryUncommittedTables copies.
type CarryPolicy int

const (
	// CarryAll carries every uncommitted table including those matched by dolt_ignore.
	CarryAll CarryPolicy = iota
	// ExcludeIgnored skips tables matched by the source's dolt_ignore patterns.
	ExcludeIgnored
)

// CarryUncommittedTables copies tables present in |src| but not in |baseline| into |dest|,
// resolving column tag collisions and carrying their foreign keys. Tables already on |dest|
// are kept unchanged. |policy| filters which uncommitted tables are eligible; see CarryPolicy.
func CarryUncommittedTables(ctx context.Context, src, baseline, dest doltdb.RootValue, policy CarryPolicy) (doltdb.RootValue, error) {
	srcSchemas, err := doltdb.GetAllSchemas(ctx, src)
	if err != nil {
		return nil, err
	}
	destSchemas, err := doltdb.GetAllSchemas(ctx, dest)
	if err != nil {
		return nil, err
	}

	uncommitted, err := uncommittedTableSchemas(ctx, src, srcSchemas, baseline, policy)
	if err != nil {
		return nil, err
	}

	dest, carried, tagRemaps, err := carryTables(ctx, src, dest, uncommitted, destSchemas)
	if err != nil {
		return nil, err
	}

	if len(carried) == 0 {
		return dest, nil
	}

	return carryForeignKeys(ctx, src, dest, carried, tagRemaps, srcSchemas, destSchemas)
}

// uncommittedTableSchemas returns the subset of |srcSchemas| whose names are absent from
// |baseline| and are not read-only dolt system tables. When |policy| is ExcludeIgnored, names
// matched by |src|'s dolt_ignore patterns are also removed. A table matched by conflicting
// dolt_ignore patterns is treated as not ignored and kept.
func uncommittedTableSchemas(ctx context.Context, src doltdb.RootValue, srcSchemas map[doltdb.TableName]schema.Schema, baseline doltdb.RootValue, policy CarryPolicy) (map[doltdb.TableName]schema.Schema, error) {
	baselineNames, err := baseline.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}
	baselineSet := doltdb.NewTableNameSet(baselineNames)

	var patternsBySchema map[string]doltdb.IgnorePatterns
	if policy == ExcludeIgnored && len(srcSchemas) > 0 {
		names := make([]doltdb.TableName, 0, len(srcSchemas))
		for n := range srcSchemas {
			names = append(names, n)
		}
		patternsBySchema, err = doltdb.GetIgnoredTablePatterns(ctx, doltdb.Roots{Working: src, Staged: src, Head: src}, doltdb.GetUniqueSchemaNamesFromTableNames(names))
		if err != nil {
			return nil, err
		}
	}

	uncommitted := make(map[doltdb.TableName]schema.Schema, len(srcSchemas))
	for name, sch := range srcSchemas {
		if baselineSet.Contains(name) || doltdb.IsReadOnlySystemTable(name) {
			continue
		}
		if patternsBySchema != nil {
			patterns := patternsBySchema[name.Schema]
			result, ignoreErr := patterns.IsTableNameIgnored(name)
			if doltdb.AsDoltIgnoreInConflict(ignoreErr) != nil {
				// Conflicting patterns: keep the table rather than guess at the intent.
			} else if ignoreErr != nil {
				return nil, ignoreErr
			} else if result == doltdb.Ignore {
				continue
			}
		}
		uncommitted[name] = sch
	}
	return uncommitted, nil
}

// carryTables copies each entry of |toCarry| from |src| into |dest|, retagging columns whose
// tag collides with one held by |dest| or by an earlier carried table. Names already on |dest|
// are skipped so the dest version wins. Returns the updated |dest|, the names that were
// carried, and the per-table tag remaps so the caller can fix up foreign keys.
func carryTables(ctx context.Context, src, dest doltdb.RootValue, toCarry map[doltdb.TableName]schema.Schema, destSchemas map[doltdb.TableName]schema.Schema) (doltdb.RootValue, []doltdb.TableName, map[doltdb.TableName]map[uint64]uint64, error) {
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
			return nil, nil, nil, fmt.Errorf("table %q does not exist in src root", name.String())
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
			return nil, nil, nil, err
		}
		carried = append(carried, name)
	}
	return dest, carried, allTagRemaps, nil
}

// carryForeignKeys copies foreign keys from |src| whose child table is in |carried| into
// |dest|'s FK collection and applies |tagRemaps| so retagged child columns stay consistent.
// Keys already present on |dest| are skipped so a pre-merged FK is not duplicated. When a
// carried key references a parent that already lives on |dest|, the referenced column tags
// are re-resolved by column name against |destSchemas| so the same column can carry a
// different internal tag on each branch and the foreign key stays valid.
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
		if destFks.Contains(fk.Name, fk.TableName) {
			return false, nil
		}
		fk.TableColumns = schema.RemapTags(fk.TableColumns, tagRemaps[fk.TableName])
		if carriedSet.Contains(fk.ReferencedTableName) {
			fk.ReferencedTableColumns = schema.RemapTags(fk.ReferencedTableColumns, tagRemaps[fk.ReferencedTableName])
		} else {
			fk.ReferencedTableColumns = schema.RemapTagsByColumnName(fk.ReferencedTableColumns, srcSchemas[fk.ReferencedTableName], destSchemas[fk.ReferencedTableName])
		}
		return false, destFks.AddKeys(fk)
	})
	if err != nil {
		return nil, err
	}
	return dest.PutForeignKeyCollection(ctx, destFks)
}
