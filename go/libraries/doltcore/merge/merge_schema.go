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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

type conflictKind byte

const (
	TagCollision conflictKind = iota
	NameCollision
)

type SchemaConflict struct {
	TableName    string
	ColConflicts []ColConflict
	IdxConflicts []IdxConflict
}

var EmptySchConflicts = SchemaConflict{}

func (sc SchemaConflict) Count() int {
	return len(sc.ColConflicts) + len(sc.IdxConflicts)
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
	return fmt.Errorf(b.String())
}

type ColConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Column
}

func (c ColConflict) String() string {
	switch c.Kind {
	case NameCollision:
		return fmt.Sprintf("two columns with the name '%s'", c.Ours.Name)
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

var ErrMergeWithDifferentPkSets = errors.New("error: cannot merge two tables with different primary key sets")

// SchemaMerge performs a three-way merge of ourSch, theirSch, and ancSch.
func SchemaMerge(ourSch, theirSch, ancSch schema.Schema, tblName string) (sch schema.Schema, sc SchemaConflict, err error) {
	// (sch - ancSch) ∪ (mergeSch - ancSch) ∪ (sch ∩ mergeSch)
	sc = SchemaConflict{
		TableName: tblName,
	}

	// TODO: We'll remove this once it's possible to get diff and merge on different primary key sets
	// TODO: decide how to merge different orders of PKS
	if !schema.ArePrimaryKeySetsDiffable(ourSch, theirSch) {
		return nil, SchemaConflict{}, ErrMergeWithDifferentPkSets
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

func mergeColumns(ourCC, theirCC, ancCC *schema.ColCollection) (merged *schema.ColCollection, conflicts []ColConflict, err error) {
	var common *schema.ColCollection
	common, conflicts = columnsInCommon(ourCC, theirCC, ancCC)

	ourNewCols := schema.ColCollectionSetDifference(ourCC, ancCC)
	theirNewCols := schema.ColCollectionSetDifference(theirCC, ancCC)

	// check for name conflicts between columns added on each branch since the ancestor
	_ = ourNewCols.Iter(func(tag uint64, ourCol schema.Column) (stop bool, err error) {
		theirCol, ok := theirNewCols.GetByNameCaseInsensitive(ourCol.Name)
		if ok && ourCol.Tag != theirCol.Tag {
			conflicts = append(conflicts, ColConflict{
				Kind:   NameCollision,
				Ours:   ourCol,
				Theirs: theirCol,
			})
		}
		return false, nil
	})

	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// order of args here is important for correct column ordering in sch schema
	// to be before any column in the intersection
	// TODO: sch column ordering doesn't respect sql "MODIFY ... AFTER ..." statements
	merged, err = schema.ColCollUnion(common, ourNewCols, theirNewCols)
	if err != nil {
		return nil, nil, err
	}

	return merged, conflicts, nil
}

func columnsInCommon(ourCC, theirCC, ancCC *schema.ColCollection) (common *schema.ColCollection, conflicts []ColConflict) {
	common = schema.NewColCollection()
	_ = ourCC.Iter(func(tag uint64, ourCol schema.Column) (stop bool, err error) {
		theirCol, ok := theirCC.GetByTag(ourCol.Tag)
		if !ok {
			return false, nil
		}

		if ourCol.Equals(theirCol) {
			common = common.Append(ourCol)
			return false, nil
		}

		ancCol, ok := ancCC.GetByTag(ourCol.Tag)
		if !ok {
			// col added on our branch and their branch with different def
			conflicts = append(conflicts, ColConflict{
				Kind:   TagCollision,
				Ours:   ourCol,
				Theirs: theirCol,
			})
			return false, nil
		}

		if ancCol.Equals(theirCol) {
			// col modified on our branch
			col, ok := common.GetByNameCaseInsensitive(ourCol.Name)
			if ok {
				conflicts = append(conflicts, ColConflict{
					Kind:   NameCollision,
					Ours:   ourCol,
					Theirs: col,
				})
			} else {
				common = common.Append(ourCol)
			}
			return false, nil
		}

		if ancCol.Equals(ourCol) {
			// col modified on their branch
			col, ok := common.GetByNameCaseInsensitive(theirCol.Name)
			if ok {
				conflicts = append(conflicts, ColConflict{
					Kind:   NameCollision,
					Ours:   col,
					Theirs: theirCol,
				})
			} else {
				common = common.Append(theirCol)
			}
			return false, nil
		}

		// col modified on our branch and their branch with different def
		conflicts = append(conflicts, ColConflict{
			Kind:   TagCollision,
			Ours:   ourCol,
			Theirs: theirCol,
		})
		return false, nil
	})

	return common, conflicts
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

		theirIdx, ok := theirs.GetIndexByTags(idxTags...)
		if !ok {
			return false, nil
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
