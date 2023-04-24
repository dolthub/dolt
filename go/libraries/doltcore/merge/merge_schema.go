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
	"errors"
	"fmt"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type conflictKind byte

const (
	TagCollision conflictKind = iota
	NameCollision
	ColumnCheckCollision
	InvalidCheckCollision
	DeletedCheckCollision
)

type SchemaConflict struct {
	TableName    string
	ColConflicts []ColConflict
	IdxConflicts []IdxConflict
	ChkConflicts []ChkConflict
}

var EmptySchConflicts = SchemaConflict{}

func (sc SchemaConflict) Count() int {
	return len(sc.ColConflicts) + len(sc.IdxConflicts) + len(sc.ChkConflicts)
}

func (sc SchemaConflict) AsError() error {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("schema conflicts for table %s:\n", sc.TableName))
	for _, c := range sc.ColConflicts {
		b.WriteString(fmt.Sprintf("\t%s\n", c.String()))
	}
	for _, c := range sc.IdxConflicts {
		b.WriteString(fmt.Sprintf("\t%s\n", c.String()))
	}
	for _, c := range sc.ChkConflicts {
		b.WriteString(fmt.Sprintf("\t%s\n", c.String()))
	}
	return fmt.Errorf(b.String())
}

type ColConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Column
}

func (c ColConflict) String() string {
	switch c.Kind {
	case NameCollision:
		return fmt.Sprintf("two columns with the same name '%s' have different tags. See https://github.com/dolthub/dolt/issues/3963", c.Ours.Name)
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
	return ""
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

var ErrMergeWithDifferentPks = errors.New("error: cannot merge two tables with different primary keys")

// SchemaMerge performs a three-way merge of ourSch, theirSch, and ancSch.
func SchemaMerge(ctx context.Context, format *types.NomsBinFormat, ourSch, theirSch, ancSch schema.Schema, tblName string) (sch schema.Schema, sc SchemaConflict, err error) {
	// (sch - ancSch) ∪ (mergeSch - ancSch) ∪ (sch ∩ mergeSch)
	sc = SchemaConflict{
		TableName: tblName,
	}

	// TODO: We'll remove this once it's possible to get diff and merge on different primary key sets
	// TODO: decide how to merge different orders of PKS
	if !schema.ArePrimaryKeySetsDiffable(format, ourSch, theirSch) || !schema.ArePrimaryKeySetsDiffable(format, ourSch, ancSch) {
		return nil, SchemaConflict{}, ErrMergeWithDifferentPks
	}

	var mergedCC *schema.ColCollection
	mergedCC, sc.ColConflicts, err = mergeColumns(ourSch.GetAllCols(), theirSch.GetAllCols(), ancSch.GetAllCols())
	if err != nil {
		return nil, EmptySchConflicts, err
	}
	if len(sc.ColConflicts) > 0 {
		return nil, sc, nil
	}

	var mergedIdxs schema.IndexCollection
	mergedIdxs, sc.IdxConflicts = mergeIndexes(mergedCC, ourSch, theirSch, ancSch)
	if len(sc.IdxConflicts) > 0 {
		return nil, sc, nil
	}

	sch, err = schema.SchemaFromCols(mergedCC)
	if err != nil {
		return nil, sc, err
	}

	// TODO: Merge conflict should have blocked any primary key ordinal changes
	err = sch.SetPkOrdinals(ourSch.GetPkOrdinals())
	if err != nil {
		return nil, sc, err
	}

	_ = mergedIdxs.Iter(func(index schema.Index) (stop bool, err error) {
		sch.Indexes().AddIndex(index)
		return false, nil
	})

	// Merge checks
	var mergedChks []schema.Check
	mergedChks, sc.ChkConflicts, err = mergeChecks(ctx, ourSch.Checks(), theirSch.Checks(), ancSch.Checks())
	if err != nil {
		return nil, EmptySchConflicts, err
	}
	if len(sc.ChkConflicts) > 0 {
		return nil, sc, nil
	}

	// Look for invalid CHECKs
	for _, chk := range mergedChks {
		// CONFLICT: a CHECK now references a column that no longer exists in schema
		if ok, err := isCheckReferenced(sch, chk); err != nil {
			return nil, sc, err
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

	return sch, sc, nil
}

// ForeignKeysMerge performs a three-way merge of (ourRoot, theirRoot, ancRoot) and using mergeRoot to validate FKs.
func ForeignKeysMerge(ctx context.Context, mergedRoot, ourRoot, theirRoot, ancRoot *doltdb.RootValue) (*doltdb.ForeignKeyCollection, []FKConflict, error) {
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

	ancSchs, err := ancRoot.GetAllSchemas(ctx)
	if err != nil {
		return nil, nil, err
	}

	common, conflicts, err := foreignKeysInCommon(ours, theirs, anc)
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
	//TODO: figure out the best way to handle unresolved foreign keys here if one branch added an unresolved one and
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
		return false, common.AddKeys(ourFK)
	})
	if err != nil {
		return nil, nil, err
	}

	err = theirNewFKs.Iter(func(theirFK doltdb.ForeignKey) (stop bool, err error) {
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

// mergeColumns merges the columns from |ourCC|, |theirCC| into a single column collection, using the ancestor column
// definitions in |ancCC| to determine on which side a column has changed. If merging is not possible because of
// conflicting changes to the columns in |ourCC| and |theirCC|, then a set of ColConflict instances are returned
// describing the conflicts. If any other, unexpected error occurs, then that error is returned and the other response
// fields should be ignored.
func mergeColumns(ourCC, theirCC, ancCC *schema.ColCollection) (*schema.ColCollection, []ColConflict, error) {
	columnMappings, err := mapColumns(ourCC, theirCC, ancCC)
	if err != nil {
		return nil, nil, err
	}

	conflicts, err := checkSchemaConflicts(columnMappings)
	if err != nil {
		return nil, nil, err
	}

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
			mergedColumns = append(mergedColumns, *theirs)
		case anc == nil && ours != nil && theirs == nil:
			// if an ancestor does not exist, and the column exists only on one side, use that side
			// (if an ancestor DOES exist, this means the column was deleted, so it's a no-op)
			mergedColumns = append(mergedColumns, *ours)
		case ours == nil && theirs == nil:
			// if the column is deleted on both sides... just let it fall out
		case ours != nil && theirs != nil:
			// otherwise, we have two valid columns and we need to figure out which one to use
			if anc != nil {
				oursChanged := !anc.Equals(*ours)
				theirsChanged := !anc.Equals(*theirs)
				if oursChanged && theirsChanged {
					// This is a schema change conflict and has already been handled by checkSchemaConflicts
				} else if theirsChanged {
					mergedColumns = append(mergedColumns, *theirs)
				} else {
					mergedColumns = append(mergedColumns, *ours)
				}
			} else if ours.Equals(*theirs) {
				// if the columns are identical, just use ours
				mergedColumns = append(mergedColumns, *ours)
			}
		}
	}

	// Check that there are no duplicate column names or tags in the merged column set
	conflicts = append(conflicts, checkForColumnConflicts(mergedColumns)...)
	if conflicts != nil {
		return nil, conflicts, nil
	}

	return schema.NewColCollection(mergedColumns...), nil, nil
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
				if !anc.Equals(*ours) {
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
				if !anc.Equals(*theirs) {
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
	for _, theirCol := range theirTagsToCols {
		ancCol, foundAncByTag := ancCC.GetByTag(theirCol.Tag)
		if !foundAncByTag {
			// Ditto for finding the ancestor column
			ancCol, _ = ancCC.GetByNameCaseInsensitive(theirCol.Name)
		}

		columnMappings = append(columnMappings, newColumnMapping(ancCol, schema.InvalidCol, theirCol))
	}

	return columnMappings, nil
}

// mergeIndexes merges the indexes from |ourSch|, |theirSch|, and |ancSch| into a single, merged collection of indexes,
// or if problems are encountered merging the indexes, then a slice of IdxConflicts are returned describing why the
// indexes could not be merged together into a single set.
func mergeIndexes(mergedCC *schema.ColCollection, ourSch, theirSch, ancSch schema.Schema) (schema.IndexCollection, []IdxConflict) {
	// Calculate the index mappings between the three schemas
	mappings := mapIndexes(ourSch.Indexes(), theirSch.Indexes(), ancSch.Indexes())
	//fmt.Printf("INDEX MAPPINGS: \n%s\n", mappings.DebugString())

	// Then look for conflicts while merging the indexes together
	var mergedIndexes []schema.Index
	var conflicts []IdxConflict
	for _, mapping := range mappings {
		if mapping.anc == nil {
			// if there's no ancestor
			switch {
			case mapping.ours == nil && mapping.theirs == nil:
				// no-op
			case mapping.ours == nil && mapping.theirs != nil:
				mergedIndexes = append(mergedIndexes, mapping.theirs)
			case mapping.ours != nil && mapping.theirs == nil:
				mergedIndexes = append(mergedIndexes, mapping.ours)
			case mapping.ours != nil && mapping.theirs != nil:
				if mapping.ours.Equals(mapping.theirs) {
					mergedIndexes = append(mergedIndexes, mapping.ours)
				} else {
					conflicts = append(conflicts, IdxConflict{
						Kind:   NameCollision,
						Ours:   mapping.ours,
						Theirs: mapping.theirs})
				}
			}
		} else {
			// if there is a common ancestor, then we need to see how each side changed from it
			switch {
			case mapping.ours == nil && mapping.theirs == nil:
				// no-op – index deleted on both sides
			case mapping.ours == nil && mapping.theirs != nil:
				if mapping.anc.Equals(mapping.theirs) == false {
					// index deleted on our side, modified on theirs – conflict
					conflicts = append(conflicts, IdxConflict{
						Kind:   NameCollision,
						Ours:   mapping.ours,
						Theirs: mapping.theirs})
				}
			case mapping.ours != nil && mapping.theirs == nil:
				if mapping.anc.Equals(mapping.ours) == false {
					// index deleted on theirs side, modified on ours – conflict
					conflicts = append(conflicts, IdxConflict{
						Kind:   NameCollision,
						Ours:   mapping.ours,
						Theirs: mapping.theirs})
				}
			case mapping.ours != nil && mapping.theirs != nil:
				oursChanged := !mapping.anc.Equals(mapping.ours)
				theirsChanged := !mapping.anc.Equals(mapping.theirs)

				if mapping.ours.Equals(mapping.theirs) {
					mergedIndexes = append(mergedIndexes, mapping.ours)
				} else {
					if !oursChanged && !theirsChanged {
						mergedIndexes = append(mergedIndexes, mapping.ours)
					} else if !oursChanged && theirsChanged {
						mergedIndexes = append(mergedIndexes, mapping.theirs)
					} else if oursChanged && !theirsChanged {
						mergedIndexes = append(mergedIndexes, mapping.ours)
					} else if oursChanged && theirsChanged {
						conflicts = append(conflicts, IdxConflict{
							Kind:   NameCollision,
							Ours:   mapping.ours,
							Theirs: mapping.theirs})
					}
				}
			}
		}
	}

	// One more sanity check for conflicting index names
	indexNames := make(map[string]struct{})
	for _, idx := range mergedIndexes {
		if _, ok := indexNames[idx.Name()]; ok {
			conflicts = append(conflicts, IdxConflict{
				Kind: NameCollision,
				Ours: idx,
			})
		} else {
			indexNames[idx.Name()] = struct{}{}
		}
	}

	mergedIndexCollection := schema.NewIndexCollection(mergedCC, nil)
	mergedIndexCollection.AddIndex(mergedIndexes...)

	return mergedIndexCollection, conflicts
}

type indexMappings []indexMapping

func (i indexMappings) DebugString() string {
	sb := strings.Builder{}
	for _, mapping := range i {
		if mapping.ours != nil {
			sb.WriteString(fmt.Sprintf("  %s ", mapping.ours.Name()))
		} else {
			sb.WriteString("  --- ")
		}
		sb.WriteString(" -> ")
		if mapping.theirs != nil {
			sb.WriteString(fmt.Sprintf("  %s ", mapping.theirs.Name()))
		} else {
			sb.WriteString("  --- ")
		}
		sb.WriteString(" -> ")
		if mapping.anc != nil {
			sb.WriteString(fmt.Sprintf("  %s ", mapping.anc.Name()))
		} else {
			sb.WriteString("  --- ")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

type indexMapping struct {
	anc, ours, theirs schema.Index
}

// TODO: godocs
func mapIndexes(ours, theirs, anc schema.IndexCollection) indexMappings {
	var seenAnc = make(map[string]struct{})
	var seenTheirs = make(map[string]struct{})
	var mappings indexMappings

	ours.Iter(func(ourIdx schema.Index) (stop bool, err error) {
		theirIndex := findMatchingIndex(ourIdx, theirs, seenTheirs)
		ancIndex := findMatchingIndex(ourIdx, anc, seenAnc)
		mappings = append(mappings, indexMapping{
			anc:    ancIndex,
			ours:   ourIdx,
			theirs: theirIndex,
		})

		return false, nil
	})

	theirs.Iter(func(theirIdx schema.Index) (stop bool, err error) {
		// Skip over any indexes from theirs that we've already matched
		if _, alreadyMatched := seenTheirs[theirIdx.Name()]; alreadyMatched {
			return false, nil
		}

		ancIndex := findMatchingIndex(theirIdx, anc, seenAnc)
		mappings = append(mappings, indexMapping{
			anc:    ancIndex,
			ours:   nil,
			theirs: theirIdx,
		})

		return false, nil
	})

	return mappings
}

// TODO: godocs
func findMatchingIndex(target schema.Index, indexCollection schema.IndexCollection, matchedNames map[string]struct{}) schema.Index {
	candidates := indexCollection.GetIndexesByTags(target.IndexedColumnTags()...)

	// First check for an exact match, including name
	for _, candidate := range candidates {
		_, alreadyMatched := matchedNames[candidate.Name()]
		if !alreadyMatched && target.Equals(candidate) {
			matchedNames[candidate.Name()] = struct{}{}
			return candidate
		}
	}

	// If we didn't find an exact match, fall back to checking for a match with a different name
	for _, candidate := range candidates {
		_, alreadyMatched := matchedNames[candidate.Name()]
		if !alreadyMatched && target.EqualsIgnoreName(candidate) {
			matchedNames[candidate.Name()] = struct{}{}
			return candidate
		}
	}

	return nil
}

func foreignKeysInCommon(ourFKs, theirFKs, ancFKs *doltdb.ForeignKeyCollection) (common *doltdb.ForeignKeyCollection, conflicts []FKConflict, err error) {
	common, _ = doltdb.NewForeignKeyCollection()
	err = ourFKs.Iter(func(ours doltdb.ForeignKey) (stop bool, err error) {
		theirs, ok := theirFKs.GetByTags(ours.TableColumns, ours.ReferencedTableColumns)
		if !ok {
			return false, nil
		}

		if theirs.EqualDefs(ours) {
			err = common.AddKeys(ours)
			return false, err
		}

		anc, ok := ancFKs.GetByTags(ours.TableColumns, ours.ReferencedTableColumns)
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
		_, ok := ancestorFkColl.GetMatchingKey(fk, ancSchs)
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
func pruneInvalidForeignKeys(ctx context.Context, fkColl *doltdb.ForeignKeyCollection, mergedRoot *doltdb.RootValue) (pruned *doltdb.ForeignKeyCollection, err error) {
	pruned, _ = doltdb.NewForeignKeyCollection()
	err = fkColl.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		parentTbl, ok, err := mergedRoot.GetTable(ctx, fk.ReferencedTableName)
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

		childTbl, ok, err := mergedRoot.GetTable(ctx, fk.TableName)
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
			common = append(common, ourChk)
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
		colNames, err := sqle.ColumnsFromCheckDefinition(sql.NewContext(ctx), &chkDef)
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
		colNames, err := sqle.ColumnsFromCheckDefinition(sql.NewContext(ctx), &chkDef)
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
	colNames, err := sqle.ColumnsFromCheckDefinition(sql.NewEmptyContext(), &chkDef)
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
