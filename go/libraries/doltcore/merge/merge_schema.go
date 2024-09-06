// Copyright 2020 Dolthub, Inc.
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

package merge

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	errorkinds "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	storetypes "github.com/dolthub/dolt/go/store/types"
)

type conflictKind byte

const (
	TagCollision conflictKind = iota
	NameCollision
	ColumnCheckCollision
	InvalidCheckCollision
	DeletedCheckCollision
	// DuplicateIndexColumnSet represent a schema conflict where multiple indexes cover the same set of columns, and
	// we're unable to accurately match them up on each side of the merge, so the user has to manually resolve.
	DuplicateIndexColumnSet
)

var ErrUnmergeableNewColumn = errorkinds.NewKind("Unable to merge new column `%s` in table `%s` because it is not-nullable and has no default value, so existing rows can't be updated automatically. To complete this merge, either manually add this new column to the target branch of the merge and update any existing rows, or change the column's definition on the other branch of the merge so that it is nullable or has a default value.")

var ErrDefaultCollationConflict = errorkinds.NewKind("Unable to merge table '%s', because its default collation setting has changed on both sides of the merge. Manually change the table's default collation setting on one of the sides of the merge and retry this merge.")

type SchemaConflict struct {
	TableName            doltdb.TableName
	ColConflicts         []ColConflict
	IdxConflicts         []IdxConflict
	ChkConflicts         []ChkConflict
	ModifyDeleteConflict bool
}

var _ error = SchemaConflict{}

func (sc SchemaConflict) Count() int {
	count := len(sc.ColConflicts) + len(sc.IdxConflicts) + len(sc.ChkConflicts)
	if sc.ModifyDeleteConflict {
		return count + 1
	}
	return count
}

// String implements fmt.Stringer. This method is used to
// display schema conflicts on schema conflict read paths.
func (sc SchemaConflict) String() string {
	return strings.Join(sc.messages(), "\n")
}

// Error implements error. This error will be returned to the
// user if merge is configured to error upon schema conflicts.
// todo: link to docs explaining how to resolve schema conflicts.
func (sc SchemaConflict) Error() string {
	template := "merge aborted: schema conflict found for table %s \n" +
		" please resolve schema conflicts before merging: %s"
	var b strings.Builder
	for _, m := range sc.messages() {
		b.WriteString("\n\t")
		b.WriteString(m)
	}
	return fmt.Sprintf(template, sc.TableName, b.String())
}

func (sc SchemaConflict) messages() (mm []string) {
	for _, c := range sc.ColConflicts {
		mm = append(mm, c.String())
	}
	for _, c := range sc.IdxConflicts {
		mm = append(mm, c.String())
	}
	for _, c := range sc.ChkConflicts {
		mm = append(mm, c.String())
	}
	if sc.ModifyDeleteConflict {
		mm = append(mm, "table was modified in one branch and deleted in the other")
	}
	return
}

type ColConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Column
}

func (c ColConflict) String() string {
	switch c.Kind {
	case NameCollision:
		return fmt.Sprintf("incompatible column types for column '%s': %s and %s", c.Ours.Name, c.Ours.TypeInfo, c.Theirs.TypeInfo)
	case TagCollision:
		return fmt.Sprintf("different column definitions for our column %s and their column %s", c.Ours.Name, c.Theirs.Name)
	}
	return ""
}

type IdxConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Index
}

func (c IdxConflict) String() string {
	switch c.Kind {
	case DuplicateIndexColumnSet:
		return fmt.Sprintf("multiple indexes covering the same column set cannot be merged: '%s' and '%s'", c.Ours.Name(), c.Theirs.Name())
	default:
		return ""
	}
}

type FKConflict struct {
	Kind         conflictKind
	Ours, Theirs doltdb.ForeignKey
}

type ChkConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Check
}

func (c ChkConflict) String() string {
	switch c.Kind {
	case NameCollision:
		return fmt.Sprintf("two checks with the name '%s' but different definitions", c.Ours.Name())
	case ColumnCheckCollision:
		return fmt.Sprintf("our check '%s' and their check '%s' both reference the same column(s)", c.Ours.Name(), c.Theirs.Name())
	case InvalidCheckCollision:
		return fmt.Sprintf("check '%s' references a column that will be deleted after merge", c.Ours.Name())
	case DeletedCheckCollision:
		if c.Theirs == nil {
			return fmt.Sprintf("check '%s' was deleted in theirs but modified in ours", c.Ours.Name())
		} else {
			return fmt.Sprintf("check '%s' was deleted in ours but modified in theirs", c.Theirs.Name())
		}
	}
	return ""
}

var ErrMergeWithDifferentPks = errorkinds.NewKind("error: cannot merge because table %s has different primary keys")
var ErrMergeWithDifferentPksFromAncestor = errorkinds.NewKind("error: cannot merge because table %s has different primary keys in its common ancestor")

// SchemaMerge performs a three-way merge of |ourSch|, |theirSch|, and |ancSch|, and returns: the merged schema,
// any schema conflicts identified, whether moving to the new schema requires a full table rewrite, and any
// unexpected error encountered while merging the schemas.
func SchemaMerge(ctx context.Context, format *storetypes.NomsBinFormat, ourSch, theirSch, ancSch schema.Schema, tblName doltdb.TableName) (sch schema.Schema, sc SchemaConflict, mergeInfo MergeInfo, diffInfo tree.ThreeWayDiffInfo, err error) {
	// (sch - ancSch) ∪ (mergeSch - ancSch) ∪ (sch ∩ mergeSch)
	sc = SchemaConflict{
		TableName: tblName,
	}

	// TODO: We'll remove this once it's possible to get diff and merge on different primary key sets
	// TODO: decide how to merge different orders of PKS
	if !schema.ArePrimaryKeySetsDiffable(format, ourSch, theirSch) {
		return nil, SchemaConflict{}, mergeInfo, diffInfo, ErrMergeWithDifferentPks.New(tblName)
	}
	if !schema.ArePrimaryKeySetsDiffable(format, ourSch, ancSch) {
		return nil, SchemaConflict{}, mergeInfo, diffInfo, ErrMergeWithDifferentPksFromAncestor.New(tblName)
	}

	var mergedCC *schema.ColCollection
	mergedCC, sc.ColConflicts, mergeInfo, diffInfo, err = mergeColumns(tblName.Name, format, ourSch.GetAllCols(), theirSch.GetAllCols(), ancSch.GetAllCols())
	if err != nil {
		return nil, SchemaConflict{}, mergeInfo, diffInfo, err
	}
	if len(sc.ColConflicts) > 0 {
		return nil, sc, mergeInfo, diffInfo, nil
	}

	var mergedIdxs schema.IndexCollection
	mergedIdxs, sc.IdxConflicts = mergeIndexes(mergedCC, ourSch, theirSch, ancSch)
	if len(sc.IdxConflicts) > 0 {
		return nil, sc, mergeInfo, diffInfo, nil
	}

	sch, err = schema.SchemaFromCols(mergedCC)
	if err != nil {
		return nil, sc, mergeInfo, diffInfo, err
	}

	sch, err = mergeTableCollation(ctx, tblName.Name, ancSch, ourSch, theirSch, sch)
	if err != nil {
		return nil, sc, mergeInfo, diffInfo, err
	}

	// TODO: Merge conflict should have blocked any primary key ordinal changes
	err = sch.SetPkOrdinals(ourSch.GetPkOrdinals())
	if err != nil {
		return nil, sc, mergeInfo, diffInfo, err
	}

	_ = mergedIdxs.Iter(func(index schema.Index) (stop bool, err error) {
		sch.Indexes().AddIndex(index)
		return false, nil
	})

	// Merge checks
	var mergedChks []schema.Check
	mergedChks, sc.ChkConflicts, err = mergeChecks(ctx, ourSch.Checks(), theirSch.Checks(), ancSch.Checks())
	if err != nil {
		return nil, SchemaConflict{}, mergeInfo, diffInfo, err
	}
	if len(sc.ChkConflicts) > 0 {
		return nil, sc, mergeInfo, diffInfo, nil
	}

	// Look for invalid CHECKs
	for _, chk := range mergedChks {
		// CONFLICT: a CHECK now references a column that no longer exists in schema
		if ok, err := isCheckReferenced(sch, chk); err != nil {
			return nil, sc, mergeInfo, diffInfo, err
		} else if !ok {
			// Append to conflicts
			sc.ChkConflicts = append(sc.ChkConflicts, ChkConflict{
				Kind: InvalidCheckCollision,
				Ours: chk,
			})
		}
	}

	// Add all merged CHECKs to merged schema
	for _, chk := range mergedChks {
		sch.Checks().AddCheck(chk.Name(), chk.Expression(), chk.Enforced())
	}

	return sch, sc, mergeInfo, diffInfo, nil
}

// ForeignKeysMerge performs a three-way merge of (ourRoot, theirRoot, ancRoot) and using mergeRoot to validate FKs.
func ForeignKeysMerge(ctx context.Context, mergedRoot, ourRoot, theirRoot, ancRoot doltdb.RootValue) (*doltdb.ForeignKeyCollection, []FKConflict, error) {
	ours, err := ourRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, nil, err
	}

	theirs, err := theirRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, nil, err
	}

	anc, err := ancRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, nil, err
	}

	ancSchs, err := doltdb.GetAllSchemas(ctx, ancRoot)
	if err != nil {
		return nil, nil, err
	}

	common, conflicts, err := foreignKeysInCommon(ours, theirs, anc, ancSchs)
	if err != nil {
		return nil, nil, err
	}

	ourNewFKs, err := fkCollSetDifference(ours, anc, ancSchs)
	if err != nil {
		return nil, nil, err
	}

	theirNewFKs, err := fkCollSetDifference(theirs, anc, ancSchs)
	if err != nil {
		return nil, nil, err
	}

	// check for conflicts between foreign keys added on each branch since the ancestor
	// TODO: figure out the best way to handle unresolved foreign keys here if one branch added an unresolved one and
	// another branch added the same one but resolved
	_ = ourNewFKs.Iter(func(ourFK doltdb.ForeignKey) (stop bool, err error) {
		theirFK, ok := theirNewFKs.GetByTags(ourFK.TableColumns, ourFK.ReferencedTableColumns)
		if ok && !ourFK.DeepEquals(theirFK) {
			// Foreign Keys are defined over the same tags,
			// but are not exactly equal
			conflicts = append(conflicts, FKConflict{
				Kind:   TagCollision,
				Ours:   ourFK,
				Theirs: theirFK,
			})
		}

		theirFK, ok = theirNewFKs.GetByNameCaseInsensitive(ourFK.Name)
		if ok && !ourFK.EqualDefs(theirFK) {
			// Two different Foreign Keys have the same name
			conflicts = append(conflicts, FKConflict{
				Kind:   NameCollision,
				Ours:   ourFK,
				Theirs: theirFK,
			})
		}
		return false, err
	})

	err = ourNewFKs.Iter(func(ourFK doltdb.ForeignKey) (stop bool, err error) {
		// The common set of FKs may already have this FK, if it was added on both branches
		if commonFK, ok := common.GetByNameCaseInsensitive(ourFK.Name); ok && commonFK.EqualDefs(ourFK) {
			// Skip this one if it's identical to the one in the common set
			return false, nil
		}

		return false, common.AddKeys(ourFK)
	})
	if err != nil {
		return nil, nil, err
	}

	err = theirNewFKs.Iter(func(theirFK doltdb.ForeignKey) (stop bool, err error) {
		// The common set of FKs may already have this FK, if it was added on both branches
		if commonFK, ok := common.GetByNameCaseInsensitive(theirFK.Name); ok && commonFK.EqualDefs(theirFK) {
			// Skip this one if it's identical to the one in the common set
			return false, nil
		}

		return false, common.AddKeys(theirFK)
	})
	if err != nil {
		return nil, nil, err
	}

	common, err = pruneInvalidForeignKeys(ctx, common, mergedRoot)
	if err != nil {
		return nil, nil, err
	}

	return common, conflicts, err
}

// checkUnmergeableNewColumns checks the |columnMappings| to see if a new column has been added that does not allow
// NULL values and has no default value, and returns an error if one is found. New, non-nullable columns that don't
// have a default value cannot be merged automatically, since we don't know what value to set for any existing rows,
// so instead of reporting a schema conflict and allowing customers to use the conflict resolution workflow, we return
// an error and instruct them how to manually fix the problem.
func checkUnmergeableNewColumns(tblName string, columnMappings columnMappings) error {
	for _, mapping := range columnMappings {
		anc := mapping.anc
		ours := mapping.ours
		theirs := mapping.theirs

		// if a new column was added...
		if anc == nil && ((ours == nil && theirs != nil) || (ours != nil && theirs == nil)) {
			newCol := ours
			if newCol == nil {
				newCol = theirs
			}

			// If the new column is not nullable and has no default value, then we can't auto merge it
			// (if there is any existing row data), so we need to error out and report the schema conflict.
			if newCol.IsNullable() == false && newCol.Default == "" {
				return ErrUnmergeableNewColumn.New(newCol, tblName)
			}
		}
	}
	return nil
}

type MergeInfo struct {
	LeftNeedsRewrite           bool
	RightNeedsRewrite          bool
	InvalidateSecondaryIndexes bool
}

// mergeColumns merges the columns from |ourCC|, |theirCC| into a single column collection, using the ancestor column
// definitions in |ancCC| to determine on which side a column has changed. If merging is not possible because of
// conflicting changes to the columns in |ourCC| and |theirCC|, then a set of ColConflict instances are returned
// describing the conflicts. |format| indicates what storage format is in use, and is needed to determine compatibility
// between types, since different storage formats have different restrictions on how much types can change and remain
// compatible with the current stored format. The merged columns, any column conflicts, and a boolean value stating if
// a full table rewrite is needed to align the existing table rows with the new, merged schema. If any unexpected error
// occurs, then that error is returned and the other response fields should be ignored.
func mergeColumns(tblName string, format *storetypes.NomsBinFormat, ourCC, theirCC, ancCC *schema.ColCollection) (*schema.ColCollection, []ColConflict, MergeInfo, tree.ThreeWayDiffInfo, error) {
	mergeInfo := MergeInfo{}
	diffInfo := tree.ThreeWayDiffInfo{}
	columnMappings, err := mapColumns(ourCC, theirCC, ancCC)
	if err != nil {
		return nil, nil, mergeInfo, diffInfo, err
	}

	conflicts, err := checkSchemaConflicts(columnMappings)
	if err != nil {
		return nil, nil, mergeInfo, diffInfo, err
	}

	err = checkUnmergeableNewColumns(tblName, columnMappings)
	if err != nil {
		return nil, nil, mergeInfo, diffInfo, err
	}

	compatChecker := newTypeCompatabilityCheckerForStorageFormat(format)

	// After we've checked for schema conflicts, merge the columns together
	// TODO: We don't currently preserve all column position changes; the returned merged columns are always based on
	//	     their position in |ourCC|, with any new columns from |theirCC| added at the end of the column collection.
	var mergedColumns []schema.Column
	for _, mapping := range columnMappings {
		ours := mapping.ours
		theirs := mapping.theirs
		anc := mapping.anc

		switch {
		case anc == nil && ours == nil && theirs != nil:
			// if an ancestor does not exist, and the column exists only on one side, use that side
			// (if an ancestor DOES exist, this means the column was deleted, so it's a no-op)
			mergeInfo.LeftNeedsRewrite = true
			diffInfo.RightSchemaChange = true
			diffInfo.LeftAndRightSchemasDiffer = true
			mergedColumns = append(mergedColumns, *theirs)
		case anc == nil && ours != nil && theirs == nil:
			// if an ancestor does not exist, and the column exists only on one side, use that side
			// (if an ancestor DOES exist, this means the column was deleted, so it's a no-op)
			mergeInfo.RightNeedsRewrite = true
			diffInfo.LeftSchemaChange = true
			diffInfo.LeftAndRightSchemasDiffer = true
			mergedColumns = append(mergedColumns, *ours)
		case anc != nil && ours == nil && theirs != nil:
			// column was deleted on our side
			mergeInfo.RightNeedsRewrite = true
			diffInfo.LeftSchemaChange = true
			diffInfo.LeftAndRightSchemasDiffer = true
		case anc != nil && ours != nil && theirs == nil:
			// column was deleted on their side
			mergeInfo.LeftNeedsRewrite = true
			diffInfo.RightSchemaChange = true
			diffInfo.LeftAndRightSchemasDiffer = true
		case ours == nil && theirs == nil:
			// if the column is deleted on both sides... just let it fall out
			diffInfo.LeftSchemaChange = true
			diffInfo.RightSchemaChange = true
		case ours != nil && theirs != nil:
			// otherwise, we have two valid columns and we need to figure out which one to use
			if anc != nil {
				oursChanged := !anc.Equals(*ours)
				theirsChanged := !anc.Equals(*theirs)
				if oursChanged && theirsChanged {
					diffInfo.LeftSchemaChange = true
					diffInfo.RightSchemaChange = true
					// If both columns changed in the same way, the modifications converge, so accept the column.
					// If not, don't report a conflict, since this case is already handled in checkForColumnConflicts.
					if ours.Equals(*theirs) {
						mergedColumns = append(mergedColumns, *theirs)
					} else {
						diffInfo.LeftAndRightSchemasDiffer = true
					}
				} else if theirsChanged {
					diffInfo.LeftAndRightSchemasDiffer = true
					// In this case, only theirsChanged, so we need to check if moving from ours->theirs
					// is valid, otherwise it's a conflict
					compatibilityInfo := compatChecker.IsTypeChangeCompatible(ours.TypeInfo, theirs.TypeInfo)
					if compatibilityInfo.invalidateSecondaryIndexes {
						mergeInfo.InvalidateSecondaryIndexes = true
					}
					if compatibilityInfo.rewriteRows {
						mergeInfo.LeftNeedsRewrite = true
						diffInfo.RightSchemaChange = true
					}
					if compatibilityInfo.compatible {
						mergedColumns = append(mergedColumns, *theirs)
					} else {
						conflicts = append(conflicts, ColConflict{
							Kind:   NameCollision,
							Ours:   *ours,
							Theirs: *theirs,
						})
					}
				} else if oursChanged {
					diffInfo.LeftAndRightSchemasDiffer = true
					// In this case, only oursChanged, so we need to check if moving from theirs->ours
					// is valid, otherwise it's a conflict
					mergeInfo.RightNeedsRewrite = true
					compatibilityInfo := compatChecker.IsTypeChangeCompatible(theirs.TypeInfo, ours.TypeInfo)
					if compatibilityInfo.invalidateSecondaryIndexes {
						mergeInfo.InvalidateSecondaryIndexes = true
					}
					if compatibilityInfo.rewriteRows {
						mergeInfo.RightNeedsRewrite = true
						diffInfo.LeftSchemaChange = true
					}
					if compatibilityInfo.compatible {
						mergedColumns = append(mergedColumns, *ours)
					} else {
						conflicts = append(conflicts, ColConflict{
							Kind:   NameCollision,
							Ours:   *ours,
							Theirs: *theirs,
						})
					}
				} else {
					// if neither side changed, just use ours
					mergedColumns = append(mergedColumns, *ours)
				}
			} else {
				// The column was added on both branches.
				diffInfo.LeftSchemaChange = true
				diffInfo.RightSchemaChange = true
				// If both columns changed in the same way, the modifications converge, so accept the column.
				// If not, don't report a conflict, since this case is already handled in checkForColumnConflicts.
				if ours.Equals(*theirs) {
					mergedColumns = append(mergedColumns, *ours)
				} else {
					diffInfo.LeftAndRightSchemasDiffer = true
				}
			}
		}
	}

	// Check that there are no duplicate column names or tags in the merged column set
	conflicts = append(conflicts, checkForColumnConflicts(mergedColumns)...)
	if conflicts != nil {
		return nil, conflicts, mergeInfo, diffInfo, nil
	}

	return schema.NewColCollection(mergedColumns...), nil, mergeInfo, diffInfo, nil
}

// checkForColumnConflicts iterates over |mergedColumns|, checks for duplicate column names or column tags, and returns
// a slice of ColConflicts for any conflicts found.
func checkForColumnConflicts(mergedColumns []schema.Column) []ColConflict {
	columnNameSet := map[string]schema.Column{}
	columnTagSet := map[uint64]schema.Column{}
	var conflicts []ColConflict

	for _, col := range mergedColumns {
		normalizedName := strings.ToLower(col.Name)
		if _, ok := columnNameSet[normalizedName]; ok {
			conflicts = append(conflicts, ColConflict{
				Kind:   NameCollision,
				Ours:   col,
				Theirs: columnNameSet[normalizedName],
			})
		}
		columnNameSet[normalizedName] = col

		if _, ok := columnTagSet[col.Tag]; ok {
			conflicts = append(conflicts, ColConflict{
				Kind:   TagCollision,
				Ours:   col,
				Theirs: columnTagSet[col.Tag],
			})
		}
		columnTagSet[col.Tag] = col
	}

	return conflicts
}

// checkSchemaConflicts iterates over |columnMappings| and returns any column schema conflicts from column changes
// that can't be automatically merged.
func checkSchemaConflicts(columnMappings columnMappings) ([]ColConflict, error) {
	var conflicts []ColConflict
	for _, mapping := range columnMappings {
		ours := mapping.ours
		theirs := mapping.theirs
		anc := mapping.anc

		// Column exists on our side
		if ours != nil {
			// If the column is identical on both sides, no need to check any more conflict cases,
			// just move on to the next column
			if theirs != nil && theirs.Equals(*ours) {
				continue
			}

			switch {
			case theirs == nil && anc != nil:
				// Column doesn't exist on their side, but does exist in ancestor
				// This means the column was deleted on theirs side
				if !anc.EqualsWithoutTag(*ours) {
					// col altered on our branch and deleted on their branch
					conflicts = append(conflicts, ColConflict{
						Kind: NameCollision,
						Ours: *ours,
					})
				}
			case theirs != nil && anc != nil:
				// Column exists on their side and in ancestor
				// If the column differs from the ancestor on both sides, then we have a conflict
				if !anc.Equals(*ours) && !anc.Equals(*theirs) {
					conflicts = append(conflicts, ColConflict{
						Kind:   TagCollision,
						Ours:   *ours,
						Theirs: *theirs,
					})
				}
			case theirs != nil && anc == nil:
				// Column exists on both sides, but not in ancestor
				// col added on our branch and their branch with different def
				conflicts = append(conflicts, ColConflict{
					Kind:   NameCollision,
					Ours:   *ours,
					Theirs: *theirs,
				})
			case theirs == nil && anc == nil:
				// column doesn't exist on theirs or in anc – no conflict
			}
		}

		// Column does not exist on our side
		if ours == nil {
			switch {
			case theirs == nil && anc != nil:
				// Column doesn't exist on their side and our side, but does exist in ancestor
				// deleted on both sides – no conflict

			case theirs != nil && anc != nil:
				// Column exists on their side and in ancestor
				// If ancs doesn't match theirs, the column was altered on both sides
				if !anc.EqualsWithoutTag(*theirs) {
					// col deleted on our branch and altered on their branch
					conflicts = append(conflicts, ColConflict{
						Kind:   NameCollision,
						Theirs: *theirs,
					})
				}

			case theirs != nil && anc == nil:
				// Column exists only on theirs; no conflict

			case theirs == nil && anc == nil:
				// Invalid for anc, ours, and theirs should never happen
				return nil, fmt.Errorf("invalid column mapping: %v", mapping)
			}
		}
	}

	return conflicts, nil
}

// columnMapping describes the mapping for a column being merged between the two sides of the merge as well as the ancestor.
type columnMapping struct {
	anc    *schema.Column
	ours   *schema.Column
	theirs *schema.Column
}

// newColumnMapping returns a new columnMapping instance, populated with the specified columns. If |anc|, |ours|,
// or |theirs| is schema.InvalidColumn (checked by looking for schema.InvalidTag), then the returned mapping will
// hold a nil value instead of schema.InvalidColumn.
func newColumnMapping(anc, ours, theirs schema.Column) columnMapping {
	var pAnc, pOurs, pTheirs *schema.Column
	if anc.Tag != schema.InvalidTag {
		pAnc = &anc
	}
	if ours.Tag != schema.InvalidTag {
		pOurs = &ours
	}
	if theirs.Tag != schema.InvalidTag {
		pTheirs = &theirs
	}
	return columnMapping{pAnc, pOurs, pTheirs}
}

type columnMappings []columnMapping

// DebugString returns a string representation of this columnMappings instance.
func (c columnMappings) DebugString() string {
	sb := strings.Builder{}

	sb.WriteString("Column Mappings:\n")
	for _, mapping := range c {
		if mapping.ours != nil {
			sb.WriteString(fmt.Sprintf("  %s (%v) ", mapping.ours.Name, mapping.ours.Tag))
		} else {
			sb.WriteString("  --- ")
		}
		sb.WriteString(" -> ")
		if mapping.theirs != nil {
			sb.WriteString(fmt.Sprintf("  %s (%v) ", mapping.theirs.Name, mapping.theirs.Tag))
		} else {
			sb.WriteString("  --- ")
		}
		sb.WriteString(" -> ")
		if mapping.anc != nil {
			sb.WriteString(fmt.Sprintf("  %s (%v) ", mapping.anc.Name, mapping.anc.Tag))
		} else {
			sb.WriteString("  --- ")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// mapColumns returns a columnMappings instance that describes how the columns in |ourCC|, |theirCC|, and |ancCC|
// map to each other.
func mapColumns(ourCC, theirCC, ancCC *schema.ColCollection) (columnMappings, error) {
	// Make a copy of theirCC so we can modify it to track which their columns we've matched
	theirCC = schema.NewColCollection(theirCC.GetColumns()...)
	theirTagsToCols := theirCC.TagToCol

	columnMappings := make(columnMappings, 0)
	_ = ourCC.Iter(func(tag uint64, ourCol schema.Column) (stop bool, err error) {
		theirCol, foundTheirByTag := theirCC.GetByTag(ourCol.Tag)
		if !foundTheirByTag {
			// If we didn't find a column on the other side of the merge that exactly matches this tag, then
			// we fallback to looking for a match by name
			theirCol, _ = theirCC.GetByNameCaseInsensitive(ourCol.Name)
		}

		ancCol, foundAncByTag := ancCC.GetByTag(ourCol.Tag)
		if !foundAncByTag {
			// Ditto for finding the ancestor column
			ancCol, _ = ancCC.GetByNameCaseInsensitive(ourCol.Name)
		}

		delete(theirTagsToCols, theirCol.Tag)
		columnMappings = append(columnMappings, newColumnMapping(ancCol, ourCol, theirCol))
		return false, nil
	})

	// Handle any remaining columns on the "their" side
	_ = theirCC.Iter(func(tag uint64, theirCol schema.Column) (stop bool, err error) {
		if _, ok := theirTagsToCols[tag]; !ok {
			return // already added
		}

		ancCol, foundAncByTag := ancCC.GetByTag(tag)
		if !foundAncByTag {
			// Ditto for finding the ancestor column
			ancCol, _ = ancCC.GetByNameCaseInsensitive(theirCol.Name)
		}
		columnMappings = append(columnMappings, newColumnMapping(ancCol, schema.InvalidCol, theirCol))
		return
	})
	return columnMappings, nil
}

// assumes indexes are unique over their column sets
func mergeIndexes(mergedCC *schema.ColCollection, ourSch, theirSch, ancSch schema.Schema) (merged schema.IndexCollection, conflicts []IdxConflict) {
	merged, conflicts = indexesInCommon(mergedCC, ourSch.Indexes(), theirSch.Indexes(), ancSch.Indexes())

	ourNewIdxs := indexCollSetDifference(ourSch.Indexes(), ancSch.Indexes(), mergedCC)
	theirNewIdxs := indexCollSetDifference(theirSch.Indexes(), ancSch.Indexes(), mergedCC)

	// check for conflicts between indexes added on each branch since the ancestor
	_ = ourNewIdxs.Iter(func(ourIdx schema.Index) (stop bool, err error) {
		theirIdx, ok := theirNewIdxs.GetByNameCaseInsensitive(ourIdx.Name())
		// If both indexes are exactly equal then there isn't a conflict
		if ok && !ourIdx.DeepEquals(theirIdx) {
			conflicts = append(conflicts, IdxConflict{
				Kind:   NameCollision,
				Ours:   ourIdx,
				Theirs: theirIdx,
			})
		}
		return false, nil
	})

	merged.AddIndex(ourNewIdxs.AllIndexes()...)
	merged.AddIndex(theirNewIdxs.AllIndexes()...)

	return merged, conflicts
}

func indexesInCommon(mergedCC *schema.ColCollection, ours, theirs, anc schema.IndexCollection) (common schema.IndexCollection, conflicts []IdxConflict) {
	common = schema.NewIndexCollection(mergedCC, nil)
	_ = ours.Iter(func(ourIdx schema.Index) (stop bool, err error) {
		idxTags := ourIdx.IndexedColumnTags()
		for _, t := range idxTags {
			// if column doesn't exist anymore, drop index
			// however, it shouldn't be possible for an index
			// over a dropped column to exist in the intersection
			if _, ok := mergedCC.GetByTag(t); !ok {
				return false, nil
			}
		}

		// Check that there aren't multiple indexes covering the same columns on "theirs"
		theirIdx, idxConflict := findIndexInCollectionByTags(ourIdx, theirs)
		if theirIdx == nil && idxConflict == nil {
			return false, nil
		} else if idxConflict != nil {
			conflicts = append(conflicts, *idxConflict)
			return true, nil
		}

		// Check that there aren't multiple indexes covering the same columns on "ours"
		_, idxConflict = findIndexInCollectionByTags(ourIdx, ours)
		if idxConflict != nil {
			conflicts = append(conflicts, *idxConflict)
			return true, nil
		}

		if ourIdx.Equals(theirIdx) {
			common.AddIndex(ourIdx)
			return false, nil
		}

		ancIdx, ok := anc.GetIndexByTags(idxTags...)

		if !ok {
			// index added on our branch and their branch with different defs, conflict
			conflicts = append(conflicts, IdxConflict{
				Kind:   TagCollision,
				Ours:   ourIdx,
				Theirs: theirIdx,
			})
			return false, nil
		}

		if ancIdx.Equals(theirIdx) {
			// index modified on our branch
			idx, ok := common.GetByNameCaseInsensitive(ourIdx.Name())
			if ok {
				conflicts = append(conflicts, IdxConflict{
					Kind:   NameCollision,
					Ours:   ourIdx,
					Theirs: idx,
				})
			} else {
				common.AddIndex(ourIdx)
			}
			return false, nil
		}

		if ancIdx.Equals(ourIdx) {
			// index modified on their branch
			idx, ok := common.GetByNameCaseInsensitive(theirIdx.Name())
			if ok {
				conflicts = append(conflicts, IdxConflict{
					Kind:   NameCollision,
					Ours:   idx,
					Theirs: theirIdx,
				})
			} else {
				common.AddIndex(theirIdx)
			}
			return false, nil
		}

		// index modified on our branch and their branch, conflict
		conflicts = append(conflicts, IdxConflict{
			Kind:   TagCollision,
			Ours:   ourIdx,
			Theirs: theirIdx,
		})
		return false, nil
	})
	return common, conflicts
}

// findIndexInCollectionByTags searches for a single index in |idxColl| that matches the same tags |idx| covers. If a
// single matching index is found, then it is returned, along with no IdxConflict. If no matching index is found, then
// nil is returned for both params. If multiple indexes are found that cover the same set of columns, a nil Index is
// returned along with an IdxConflict that describes the conflict.
//
// Dolt allows you to add multiple indexes that cover the same set of columns, but in this situation, we aren't able
// to always accurately match up the indexes between ours/theirs/anc in a merge. The set of column tags an
// index covers was being used as a unique ID for the index, but as our index support has grown and in order to match
// MySQL's behavior, this isn't guaranteed to be a unique identifier anymore.
func findIndexInCollectionByTags(idx schema.Index, idxColl schema.IndexCollection) (schema.Index, *IdxConflict) {
	theirIdxs := idxColl.GetIndexesByTags(idx.IndexedColumnTags()...)
	switch len(theirIdxs) {
	case 0:
		return nil, nil
	case 1:
		return theirIdxs[0], nil
	default:
		sort.Slice(theirIdxs, func(i, j int) bool {
			return theirIdxs[i].Name() < theirIdxs[j].Name()
		})

		return nil, &IdxConflict{
			Kind:   DuplicateIndexColumnSet,
			Ours:   theirIdxs[0],
			Theirs: theirIdxs[1],
		}
	}
}

func indexCollSetDifference(left, right schema.IndexCollection, cc *schema.ColCollection) (d schema.IndexCollection) {
	d = schema.NewIndexCollection(cc, nil)
	_ = left.Iter(func(idx schema.Index) (stop bool, err error) {
		idxTags := idx.IndexedColumnTags()
		for _, t := range idxTags {
			// if column doesn't exist anymore, drop index
			if _, ok := cc.GetByTag(t); !ok {
				return false, nil
			}
		}

		_, ok := right.GetIndexByTags(idxTags...)
		if !ok {
			d.AddIndex(idx)
		}
		return false, nil
	})
	return d
}

func foreignKeysInCommon(ourFKs, theirFKs, ancFKs *doltdb.ForeignKeyCollection, ancSchs map[string]schema.Schema) (common *doltdb.ForeignKeyCollection, conflicts []FKConflict, err error) {
	common, _ = doltdb.NewForeignKeyCollection()
	err = ourFKs.Iter(func(ours doltdb.ForeignKey) (stop bool, err error) {

		// Since we aren't using an ancestor root here, pass true for the
		// matchUnresolvedKeyToResolvedKey parameter. This allows us to match
		// resolved FKs with both resolved and unresolved FKs in theirFKs.
		// See GetMatchingKey's documentation for more info.
		theirs, ok := theirFKs.GetMatchingKey(ours, ancSchs, true)
		if !ok {
			return false, nil
		}

		if theirs.EqualDefs(ours) {
			err = common.AddKeys(ours)
			return false, err
		}

		anc, ok := ancFKs.GetMatchingKey(ours, ancSchs, false)
		if !ok {
			// FKs added on both branch with different defs
			conflicts = append(conflicts, FKConflict{
				Kind:   TagCollision,
				Ours:   ours,
				Theirs: theirs,
			})
		}

		if theirs.EqualDefs(anc) {
			// FK modified on our branch since the ancestor
			fk, ok := common.GetByNameCaseInsensitive(ours.Name)
			if ok {
				conflicts = append(conflicts, FKConflict{
					Kind:   NameCollision,
					Ours:   ours,
					Theirs: fk,
				})
			} else {
				err = common.AddKeys(ours)
			}
			return false, err
		}

		if ours.EqualDefs(anc) {
			// FK modified on their branch since the ancestor
			fk, ok := common.GetByNameCaseInsensitive(theirs.Name)
			if ok {
				conflicts = append(conflicts, FKConflict{
					Kind:   NameCollision,
					Ours:   fk,
					Theirs: theirs,
				})
			} else {
				err = common.AddKeys(theirs)
			}
			return false, err
		}

		// FKs modified on both branch with different defs
		conflicts = append(conflicts, FKConflict{
			Kind:   TagCollision,
			Ours:   ours,
			Theirs: theirs,
		})
		return false, nil
	})

	if err != nil {
		return nil, nil, err
	}

	return common, conflicts, nil
}

// fkCollSetDifference returns a collection of all foreign keys that are in the given collection but not the ancestor
// collection. This is specifically for finding differences between a descendant and an ancestor, and therefore should
// not be used in the general case.
func fkCollSetDifference(fkColl, ancestorFkColl *doltdb.ForeignKeyCollection, ancSchs map[string]schema.Schema) (d *doltdb.ForeignKeyCollection, err error) {
	d, _ = doltdb.NewForeignKeyCollection()
	err = fkColl.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		_, ok := ancestorFkColl.GetMatchingKey(fk, ancSchs, false)
		if !ok {
			err = d.AddKeys(fk)
		}
		return false, err
	})

	if err != nil {
		return nil, err
	}

	return d, nil
}

// pruneInvalidForeignKeys removes from a ForeignKeyCollection any ForeignKey whose parent/child table/columns have been removed.
func pruneInvalidForeignKeys(ctx context.Context, fkColl *doltdb.ForeignKeyCollection, mergedRoot doltdb.RootValue) (pruned *doltdb.ForeignKeyCollection, err error) {
	pruned, _ = doltdb.NewForeignKeyCollection()
	err = fkColl.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		parentTbl, ok, err := mergedRoot.GetTable(ctx, doltdb.TableName{Name: fk.ReferencedTableName})
		if err != nil || !ok {
			return false, err
		}
		parentSch, err := parentTbl.GetSchema(ctx)
		if err != nil {
			return false, err
		}
		for _, tag := range fk.ReferencedTableColumns {
			if _, ok := parentSch.GetAllCols().GetByTag(tag); !ok {
				return false, nil
			}
		}

		childTbl, ok, err := mergedRoot.GetTable(ctx, doltdb.TableName{Name: fk.TableName})
		if err != nil || !ok {
			return false, err
		}
		childSch, err := childTbl.GetSchema(ctx)
		if err != nil {
			return false, err
		}
		for _, tag := range fk.TableColumns {
			if _, ok := childSch.GetAllCols().GetByTag(tag); !ok {
				return false, nil
			}
		}

		err = pruned.AddKeys(fk)
		return false, err
	})

	if err != nil {
		return nil, err
	}

	return pruned, nil
}

// checksInCommon finds all the common checks between ourChks, theirChks, and ancChks, and detects varying conflicts
func checksInCommon(ourChks, theirChks, ancChks []schema.Check) ([]schema.Check, []ChkConflict) {
	// Make map of their checks for fast lookup
	theirChkMap := make(map[string]schema.Check)
	for _, chk := range theirChks {
		theirChkMap[chk.Name()] = chk
	}

	// Make map of ancestor checks for fast lookup
	ancChkMap := make(map[string]schema.Check)
	for _, chk := range ancChks {
		ancChkMap[chk.Name()] = chk
	}

	// Iterate over our checks
	var common []schema.Check
	var conflicts []ChkConflict
	for _, ourChk := range ourChks {
		// See if ours and theirs both have a CHECK by this name
		theirChk, ok := theirChkMap[ourChk.Name()]
		// Ours and theirs do have this CHECK in common, will be dealt with elsewhere
		if !ok {
			continue
		}

		// NO CONFLICT: our and their check are defined exactly the same
		if ourChk == theirChk {
			common = append(common, ourChk)
			continue
		}

		// See if ancestor also has this check
		ancChk, ok := ancChkMap[ourChk.Name()]
		// CONFLICT: our and their CHECK have the same name, but different definitions
		if !ok {
			conflicts = append(conflicts, ChkConflict{
				Kind:   NameCollision,
				Ours:   ourChk,
				Theirs: theirChk,
			})
			continue
		}

		// NO CONFLICT: CHECK was only modified in our branch, so update check definition with ours
		if ancChk == theirChk {
			common = append(common, ourChk)
			continue
		}

		// NO CONFLICT: CHECK was only modified in their branch, so update check definition with theirs
		if ancChk == ourChk {
			common = append(common, theirChk)
			continue
		}

		// CONFLICT: CHECK was modified on both
		conflicts = append(conflicts, ChkConflict{
			Kind:   NameCollision,
			Ours:   ourChk,
			Theirs: theirChk,
		})
	}

	return common, conflicts
}

// chkCollectionSetDifference returns the set difference left - right.
func chkCollectionSetDifference(left, right []schema.Check) []schema.Check {
	// Make map of right check for fast look up
	rChkMap := make(map[string]bool)
	for _, chk := range right {
		rChkMap[chk.Name()] = true
	}

	// Add everything except what's in right
	var result []schema.Check
	for _, chk := range left {
		if _, ok := rChkMap[chk.Name()]; ok {
			continue
		}
		result = append(result, chk)
	}
	return result
}

// chkCollectionSetIntersection returns the set union of left and right.
func chkCollectionSetIntersection(left, right []schema.Check) []schema.Check {
	// Make map of right check for fast look up
	rChkMap := make(map[string]bool)
	for _, chk := range right {
		rChkMap[chk.Name()] = true
	}

	// Add everything from left that is also in right
	var result []schema.Check
	for _, chk := range left {
		if _, ok := rChkMap[chk.Name()]; !ok {
			continue
		}
		result = append(result, chk)
	}
	return result
}

// chkCollectionModified finds all checks that have been modified from ancestor to child
func chkCollectionModified(anc, child []schema.Check) []schema.Check {
	// Make map of ancestor for fast look up
	ancChkMap := make(map[string]schema.Check)
	for _, chk := range anc {
		ancChkMap[chk.Name()] = chk
	}

	// Add everything with same name, but different definition
	var result []schema.Check
	for _, childChk := range child {
		if ancChk, ok := ancChkMap[childChk.Name()]; ok {
			if ancChk != childChk {
				result = append(result, childChk)
			}
		}
	}
	return result
}

// mergeTableCollation checks how the table's default collation setting has changed from |ancSch| to |ourSch|, as
// well as from |ancSch| to |theirSch|, and then sets the collation in |mergedSch| and returns it. If the default
// table collation setting was changed on both sides of the merge (to different collations), then an error is returned.
func mergeTableCollation(_ context.Context, tblName string, ancSch, ourSch, theirSch, mergedSch schema.Schema) (schema.Schema, error) {
	// Update the default charset/collation setting if it changed on only one side
	ourCollationChanged := ancSch != nil && ancSch.GetCollation() != ourSch.GetCollation()
	theirCollationChanged := ancSch != nil && ancSch.GetCollation() != theirSch.GetCollation()

	if ourCollationChanged && theirCollationChanged && ourSch.GetCollation() != theirSch.GetCollation() {
		return nil, ErrDefaultCollationConflict.New(tblName)
	}
	mergedSch.SetCollation(ourSch.GetCollation())
	if theirCollationChanged {
		mergedSch.SetCollation(theirSch.GetCollation())
	}

	return mergedSch, nil
}

// mergeChecks attempts to combine ourChks, theirChks, and ancChks into a single collection, or gathers the conflicts
func mergeChecks(ctx context.Context, ourChks, theirChks, ancChks schema.CheckCollection) ([]schema.Check, []ChkConflict, error) {
	// Handles modifications
	common, conflicts := checksInCommon(ourChks.AllChecks(), theirChks.AllChecks(), ancChks.AllChecks())

	// Get all new checks
	ourNewChks := chkCollectionSetDifference(ourChks.AllChecks(), ancChks.AllChecks())
	theirNewChks := chkCollectionSetDifference(theirChks.AllChecks(), ancChks.AllChecks())

	// Create map for fast lookup
	theirNewChksMap := make(map[string]schema.Check)
	for _, chk := range theirNewChks {
		theirNewChksMap[chk.Name()] = chk
	}

	// Compare CHECKs with the same name
	for _, ourChk := range ourNewChks {
		theirChk, ok := theirNewChksMap[ourChk.Name()]
		// CONFLICT: our and their CHECK have the same name, but different definitions
		if ok && ourChk != theirChk {
			conflicts = append(conflicts, ChkConflict{
				Kind:   NameCollision,
				Ours:   ourChk,
				Theirs: theirChk,
			})
		}
	}

	// There are conflicts, don't merge
	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// Create a map from each column to any CHECKs that reference that column
	theirNewChkColsMap := make(map[string]map[schema.Check]bool)
	for _, chk := range theirNewChks {
		// Extract columns referenced by CHECK
		chkDef := sql.CheckDefinition{
			Name:            chk.Name(),
			CheckExpression: chk.Expression(),
			Enforced:        chk.Enforced(),
		}
		colNames, err := ColumnsFromCheckDefinition(sql.NewContext(ctx), &chkDef)
		if err != nil {
			return nil, nil, err
		}

		// Mark that col as referenced by CHECK
		for _, col := range colNames {
			if _, ok := theirNewChkColsMap[col]; !ok {
				theirNewChkColsMap[col] = make(map[schema.Check]bool)
			}
			theirNewChkColsMap[col][chk] = true
		}
	}

	// Look for overlapping columns between our new CHECKs and their new CHECKs
	for _, ourChk := range ourNewChks {
		// Extract columns referenced by CHECK
		chkDef := sql.CheckDefinition{
			Name:            ourChk.Name(),
			CheckExpression: ourChk.Expression(),
			Enforced:        ourChk.Enforced(),
		}
		colNames, err := ColumnsFromCheckDefinition(sql.NewContext(ctx), &chkDef)
		if err != nil {
			return nil, nil, err
		}

		// TODO: redundant for checks that are defined exactly the same
		for _, col := range colNames {
			// See if this column is referenced in their new CHECKs
			if _, ok := theirNewChkColsMap[col]; ok {
				// CONFLICT: our and their CHECK reference the same column and are not the same CHECK
				if _, ok := theirNewChkColsMap[col][ourChk]; !ok {
					for k := range theirNewChkColsMap[col] {
						conflicts = append(conflicts, ChkConflict{
							Kind:   ColumnCheckCollision,
							Ours:   ourChk,
							Theirs: k,
						})
					}
					// Finding one column collision is enough
					break
				}
			}
		}
	}

	// There are conflicts, don't merge
	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// CONFLICT: deleted constraint in ours that is modified in theirs
	ourDeletedChks := chkCollectionSetDifference(ancChks.AllChecks(), ourChks.AllChecks())
	theirModifiedChks := chkCollectionModified(ancChks.AllChecks(), theirChks.AllChecks())
	deletedInOursButModifiedInTheirs := chkCollectionSetIntersection(theirModifiedChks, ourDeletedChks)
	for _, chk := range deletedInOursButModifiedInTheirs {
		conflicts = append(conflicts, ChkConflict{
			Kind:   DeletedCheckCollision,
			Theirs: chk,
		})
	}

	// CONFLICT: deleted constraint in theirs that is modified in ours
	theirDeletedChks := chkCollectionSetDifference(ancChks.AllChecks(), theirChks.AllChecks())
	ourModifiedChks := chkCollectionModified(ancChks.AllChecks(), ourChks.AllChecks())
	deletedInTheirsButModifiedInOurs := chkCollectionSetIntersection(ourModifiedChks, theirDeletedChks)
	for _, chk := range deletedInTheirsButModifiedInOurs {
		conflicts = append(conflicts, ChkConflict{
			Kind: DeletedCheckCollision,
			Ours: chk,
		})
	}

	// There are conflicts, don't merge
	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// Create map to track names
	var allChecks []schema.Check
	allNames := make(map[string]bool)

	// Combine all checks into one collection
	for _, chk := range common {
		if _, ok := allNames[chk.Name()]; !ok {
			allNames[chk.Name()] = true
			allChecks = append(allChecks, chk)
		}
	}
	for _, chk := range ourNewChks {
		if _, ok := allNames[chk.Name()]; !ok {
			allNames[chk.Name()] = true
			allChecks = append(allChecks, chk)
		}
	}
	for _, chk := range theirNewChks {
		if _, ok := allNames[chk.Name()]; !ok {
			allNames[chk.Name()] = true
			allChecks = append(allChecks, chk)
		}
	}

	return allChecks, conflicts, nil
}

// isCheckReferenced determine if columns referenced in check are in schema
func isCheckReferenced(sch schema.Schema, chk schema.Check) (bool, error) {
	chkDef := sql.CheckDefinition{
		Name:            chk.Name(),
		CheckExpression: chk.Expression(),
		Enforced:        chk.Enforced(),
	}
	colNames, err := ColumnsFromCheckDefinition(sql.NewEmptyContext(), &chkDef)
	if err != nil {
		return false, err
	}

	for _, col := range colNames {
		_, ok := sch.GetAllCols().GetByName(col)
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

// ColumnsFromCheckDefinition retrieves the Column Names referenced by a CheckDefinition
func ColumnsFromCheckDefinition(ctx *sql.Context, def *sql.CheckDefinition) ([]string, error) {
	// Evaluate the CheckDefinition to get evaluated Expression
	parseStr := fmt.Sprintf("select %s", def.CheckExpression)
	parsed, err := ast.Parse(parseStr)
	if err != nil {
		return nil, err
	}

	selectStmt, ok := parsed.(*ast.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		err := sql.ErrInvalidCheckConstraint.New(def.CheckExpression)
		return nil, err
	}

	expr := selectStmt.SelectExprs[0]
	ae, ok := expr.(*ast.AliasedExpr)
	if !ok {
		err := sql.ErrInvalidCheckConstraint.New(def.CheckExpression)
		return nil, err
	}

	// Look for any column references in the evaluated Expression
	var cols []string
	ast.Walk(func(n ast.SQLNode) (kontinue bool, err error) {
		switch n := n.(type) {
		case *ast.ColName:
			colName := n.Name.Lowered()
			cols = append(cols, colName)
		default:
		}
		return true, nil
	}, ae.Expr)
	return cols, nil
}
