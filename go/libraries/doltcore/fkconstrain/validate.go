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

package fkconstrain

import (
	"context"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	nomsdiff "github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

func Validate(ctx context.Context, parentCommitRoot, root *doltdb.RootValue) error {
	tblNames, err := root.GetTableNames(ctx)

	if err != nil {
		return err
	}

	tblNameToValidationInfo, err := getFKValidationInfo(ctx, root, tblNames)

	if err != nil {
		return err
	}

	for _, tblName := range tblNames {
		// skip tables that don't have foreign key constraints
		validationInfo, ok := tblNameToValidationInfo[tblName]

		if !ok || len(validationInfo.allChecks) == 0 {
			continue
		}

		// get iterator that, when possible, only iterates over changes
		diffItr, err := getDiffItr(ctx, parentCommitRoot, root, tblName)

		if err != nil {
			return err
		}

		err = validateFKForDiffs(ctx, diffItr, validationInfo)

		if err != nil {
			return err
		}
	}

	return nil
}

func validateFKForDiffs(ctx context.Context, itr diff.RowDiffer, info fkValidationInfo) error {
	for {
		diffs, ok, err := itr.GetDiffs(1, time.Minute)

		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		d := diffs[0]
		switch d.ChangeType {
		case types.DiffChangeRemoved:
			if len(info.referencedFK) == 0 {
				break
			}

			tv, err := row.TaggedValuesFromTupleKeyAndValue(d.KeyValue.(types.Tuple), d.OldValue.(types.Tuple))

			if err != nil {
				return err
			}

			// when a row is removed we need to check that no rows were referencing that value
			for _, check := range info.referencedFK {
				err = check.Check(ctx, tv, nil)

				if err != nil {
					return err
				}
			}

		case types.DiffChangeAdded:
			if len(info.declaredFK) == 0 {
				break
			}

			tv, err := row.TaggedValuesFromTupleKeyAndValue(d.KeyValue.(types.Tuple), d.NewValue.(types.Tuple))

			if err != nil {
				return err
			}

			// when a row is added we need to check that all the foreign key constraints declared on the table
			// are satisfied
			for _, check := range info.declaredFK {
				err = check.Check(ctx, nil, tv)

				if err != nil {
					return err
				}
			}

		case types.DiffChangeModified:
			oldTV, newTV, colsChanged, err := parseDiff(d)

			if err != nil {
				return err
			}

			for _, check := range info.allChecks {
				if check.ColsIntersectChanges(colsChanged) {
					err = check.Check(ctx, oldTV, newTV)

					if err != nil {
						return err
					}
				}
			}
		}
	}
}

func nextTagAndValue(itr *types.TupleIterator) (uint64, types.Value, error) {
	_, tag, err := itr.NextUint64()

	if err != nil {
		return 0, nil, err
	}

	_, val, err := itr.Next()

	if err != nil {
		return 0, nil, err
	}

	return tag, val, nil
}

func parseDiff(d *nomsdiff.Difference) (oldTV, newTV row.TaggedValues, changes map[uint64]bool, err error) {
	const MaxTag uint64 = (1 << 64) - 1

	newTV = make(row.TaggedValues)
	oldTV = make(row.TaggedValues)
	changes = make(map[uint64]bool)

	itr, err := d.KeyValue.(types.Tuple).Iterator()

	if err != nil {
		return nil, nil, nil, err
	}

	for itr.HasMore() {
		tag, val, err := nextTagAndValue(itr)

		if err != nil {
			return nil, nil, nil, err
		}

		newTV[tag] = val
		oldTV[tag] = val
	}

	oldVal := d.OldValue.(types.Tuple)
	newVal := d.NewValue.(types.Tuple)

	oldItr, err := oldVal.Iterator()

	if err != nil {
		return nil, nil, nil, err
	}

	newItr, err := newVal.Iterator()

	if err != nil {
		return nil, nil, nil, err
	}

	var currNewTag, currOldTag uint64
	var currNewVal, currOldVal types.Value
	for {
		if currNewVal == nil {
			if !newItr.HasMore() {
				currNewTag = MaxTag
			} else {
				currNewTag, currNewVal, err = nextTagAndValue(newItr)
				if err != nil {
					return nil, nil, nil, err
				}
			}
		}

		if currOldVal == nil {
			if !oldItr.HasMore() {
				if currNewTag == MaxTag {
					break
				}

				currOldTag = MaxTag
			} else {
				currOldTag, currOldVal, err = nextTagAndValue(oldItr)
				if err != nil {
					return nil, nil, nil, err
				}
			}
		}

		if currNewTag < currOldTag {
			newTV[currNewTag] = currNewVal
			oldTV[currNewTag] = types.NullValue
			changes[currNewTag] = true
			currNewVal = nil
		} else if currOldTag < currNewTag {
			newTV[currOldTag] = types.NullValue
			oldTV[currOldTag] = currOldVal
			changes[currOldTag] = true
			currOldVal = nil
		} else {
			newTV[currNewTag] = currNewVal
			oldTV[currOldTag] = currOldVal
			changes[currNewTag] = !currOldVal.Equals(currNewVal)
			currNewVal, currOldVal = nil, nil
		}
	}

	return oldTV, newTV, changes, nil
}

type fkValidationInfo struct {
	declaredFK   []declaredFKCheck
	referencedFK []referencedFKCheck
	allChecks    []fkCheck
}

func getFKValidationInfo(ctx context.Context, root *doltdb.RootValue, tblNames []string) (map[string]fkValidationInfo, error) {
	tblToValInfo := make(map[string]fkValidationInfo)

	fkColl, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	for _, tblName := range tblNames {
		declaredFk, referencedByFk := fkColl.KeysForTable(tblName)

		declaredFKChecks := make([]declaredFKCheck, 0, len(declaredFk))
		referencedFKChecks := make([]referencedFKCheck, 0, len(referencedByFk))
		allFKChecks := make([]fkCheck, 0, len(declaredFKChecks)+len(referencedFKChecks))

		for _, dfk := range declaredFk {
			chk, err := newDeclaredFKCheck(ctx, root, dfk)

			if err != nil {
				return nil, err
			}

			declaredFKChecks = append(declaredFKChecks, chk)
			allFKChecks = append(allFKChecks, chk)
		}

		for _, rfk := range referencedByFk {
			chk, err := newRefFKCheck(ctx, root, rfk)

			if err != nil {
				return nil, err
			}

			referencedFKChecks = append(referencedFKChecks, chk)
			allFKChecks = append(allFKChecks, chk)
		}

		tblToValInfo[tblName] = fkValidationInfo{
			declaredFK:   declaredFKChecks,
			referencedFK: referencedFKChecks,
			allChecks:    allFKChecks,
		}
	}

	return tblToValInfo, nil
}
