// Copyright 2023 Dolthub, Inc.
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
	"bytes"
	"context"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"
)

type ThreeWayJsonDiffer struct {
	leftDiffer, rightDiffer           tree.IJsonDiffer
	leftCurrentDiff, rightCurrentDiff *tree.JsonDiff
	leftIsDone, rightIsDone           bool
	ns                                tree.NodeStore
}

type ThreeWayJsonDiff struct {
	// Op indicates the type of diff
	Op tree.DiffOp
	// a partial set of document values are set
	// depending on the diffOp
	Key                 []byte
	Left, Right, Merged sql.JSONWrapper
}

func (differ *ThreeWayJsonDiffer) Next(ctx context.Context) (ThreeWayJsonDiff, error) {
	for {
		err := differ.loadNextDiff(ctx)
		if err != nil {
			return ThreeWayJsonDiff{}, err
		}

		if differ.rightIsDone {
			// We don't care if there are remaining left diffs because they don't affect the merge.
			return ThreeWayJsonDiff{}, io.EOF
		}

		if differ.leftIsDone {
			return differ.processRightSideOnlyDiff(), nil
		}

		// !differ.rightIsDone && !differ.leftIsDone
		leftDiff := differ.leftCurrentDiff
		rightDiff := differ.rightCurrentDiff
		leftKey := leftDiff.Key
		rightKey := rightDiff.Key

		cmp := bytes.Compare(leftKey, rightKey)
		// If both sides modify the same array to different values, we currently consider that to be a conflict.
		// This may be relaxed in the future.
		if cmp != 0 && tree.JsonKeysModifySameArray(leftKey, rightKey) {
			result := ThreeWayJsonDiff{
				Op: tree.DiffOpDivergentModifyConflict,
			}
			return result, nil
		}
		if cmp > 0 {
			if tree.IsJsonKeyPrefix(leftKey, rightKey) {
				// The right diff must be replacing or deleting an object,
				// and the left diff makes changes to that object.
				result := ThreeWayJsonDiff{
					Op: tree.DiffOpDivergentModifyConflict,
				}
				differ.leftCurrentDiff = nil
				return result, nil
			}
			// key only changed on right
			return differ.processRightSideOnlyDiff(), nil
		} else if cmp < 0 {
			if tree.IsJsonKeyPrefix(rightKey, leftKey) {
				// The right diff must be replacing or deleting an object,
				// and the left diff makes changes to that object.
				result := ThreeWayJsonDiff{
					Op: tree.DiffOpDivergentModifyConflict,
				}
				differ.rightCurrentDiff = nil
				return result, nil
			}
			// left side was modified. We don't need to do anything with this diff.
			differ.leftCurrentDiff = nil
			continue
		} else {
			// cmp == 0; Both diffs are on the same key
			if differ.leftCurrentDiff.From == nil {
				// Key did not exist at base, so both sides are inserts.
				// Check that they're inserting the same value.
				valueCmp, err := types.CompareJSON(differ.leftCurrentDiff.To, differ.rightCurrentDiff.To)
				if err != nil {
					return ThreeWayJsonDiff{}, err
				}
				if valueCmp == 0 {
					return differ.processMergedDiff(tree.DiffOpConvergentAdd, differ.leftCurrentDiff.To), nil
				} else {
					return differ.processMergedDiff(tree.DiffOpDivergentModifyConflict, nil), nil
				}
			}
			if differ.leftCurrentDiff.To == nil && differ.rightCurrentDiff.To == nil {
				return differ.processMergedDiff(tree.DiffOpConvergentDelete, nil), nil
			}
			if differ.leftCurrentDiff.To == nil || differ.rightCurrentDiff.To == nil {
				return differ.processMergedDiff(tree.DiffOpDivergentDeleteConflict, nil), nil
			}
			// If the key existed at base, we can do a recursive three-way merge to resolve
			// changes to the values.
			// This shouldn't be necessary: if its an object on all three branches, the original diff is recursive.
			mergedValue, conflict, err := mergeJSON(ctx, differ.ns, differ.leftCurrentDiff.From,
				differ.leftCurrentDiff.To,
				differ.rightCurrentDiff.To)
			if err != nil {
				return ThreeWayJsonDiff{}, err
			}
			if conflict {
				return differ.processMergedDiff(tree.DiffOpDivergentModifyConflict, nil), nil
			} else {
				return differ.processMergedDiff(tree.DiffOpDivergentModifyResolved, mergedValue), nil
			}
		}
	}
}

func (differ *ThreeWayJsonDiffer) loadNextDiff(ctx context.Context) error {
	if differ.leftCurrentDiff == nil && !differ.leftIsDone {
		newLeftDiff, err := differ.leftDiffer.Next(ctx)
		if err == io.EOF {
			differ.leftIsDone = true
		} else if err != nil {
			return err
		} else {
			differ.leftCurrentDiff = &newLeftDiff
		}
	}
	if differ.rightCurrentDiff == nil && !differ.rightIsDone {
		newRightDiff, err := differ.rightDiffer.Next(ctx)
		if err == io.EOF {
			differ.rightIsDone = true
		} else if err != nil {
			return err
		} else {
			differ.rightCurrentDiff = &newRightDiff
		}
	}
	return nil
}

func (differ *ThreeWayJsonDiffer) processRightSideOnlyDiff() ThreeWayJsonDiff {
	switch differ.rightCurrentDiff.Type {
	case tree.AddedDiff:
		result := ThreeWayJsonDiff{
			Op:    tree.DiffOpRightAdd,
			Key:   differ.rightCurrentDiff.Key,
			Right: differ.rightCurrentDiff.To,
		}
		differ.rightCurrentDiff = nil
		return result

	case tree.RemovedDiff:
		result := ThreeWayJsonDiff{
			Op:  tree.DiffOpRightDelete,
			Key: differ.rightCurrentDiff.Key,
		}
		differ.rightCurrentDiff = nil
		return result

	case tree.ModifiedDiff:
		result := ThreeWayJsonDiff{
			Op:    tree.DiffOpRightModify,
			Key:   differ.rightCurrentDiff.Key,
			Right: differ.rightCurrentDiff.To,
		}
		differ.rightCurrentDiff = nil
		return result
	default:
		panic("unreachable")
	}
}

func (differ *ThreeWayJsonDiffer) processMergedDiff(op tree.DiffOp, merged sql.JSONWrapper) ThreeWayJsonDiff {
	result := ThreeWayJsonDiff{
		Op:     op,
		Key:    differ.leftCurrentDiff.Key,
		Left:   differ.leftCurrentDiff.To,
		Right:  differ.rightCurrentDiff.To,
		Merged: merged,
	}
	differ.leftCurrentDiff = nil
	differ.rightCurrentDiff = nil
	return result
}
