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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/prolly/tree"
)

type ThreeWayJsonDiffer struct {
	leftDiffer, rightDiffer           tree.JsonDiffer
	leftCurrentDiff, rightCurrentDiff *tree.JsonDiff
	lastMatchedArrayPath              []byte
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

		var leftKey, rightKey []byte
		if differ.leftCurrentDiff != nil {
			leftKey = differ.leftCurrentDiff.Key
		}
		if differ.rightCurrentDiff != nil {
			rightKey = differ.rightCurrentDiff.Key
		}
		leftArray, rightArray := tree.JsonArrayPath(leftKey), tree.JsonArrayPath(rightKey)

		// A shared element change can be followed by another change on only one side.
		// Check for that conflict even when the other diff stream has ended.
		if len(differ.lastMatchedArrayPath) > 0 {
			leftStillInMatchedArray := bytes.Equal(leftArray, differ.lastMatchedArrayPath)
			rightStillInMatchedArray := bytes.Equal(rightArray, differ.lastMatchedArrayPath)
			if leftStillInMatchedArray != rightStillInMatchedArray {
				return ThreeWayJsonDiff{Op: tree.DiffOpDivergentModifyConflict}, nil
			}
			if !leftStillInMatchedArray {
				differ.lastMatchedArrayPath = differ.lastMatchedArrayPath[:0]
			}
		}

		if differ.rightIsDone {
			return ThreeWayJsonDiff{}, io.EOF
		}
		if differ.leftIsDone {
			return differ.processRightSideOnlyDiff(), nil
		}

		cmp := bytes.Compare(leftKey, rightKey)
		// If both sides modify the same array to different values, we currently consider that to be a conflict.
		// This may be relaxed in the future.
		if cmp != 0 && leftArray != nil && bytes.Equal(leftArray, rightArray) {
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
			// Both diffs are on the same key. Copy the array path because the
			// element diff may borrow its key from the differ's working buffer.
			differ.lastMatchedArrayPath = append(differ.lastMatchedArrayPath[:0], leftArray...)
			if differ.leftCurrentDiff.To == nil && differ.rightCurrentDiff.To == nil {
				return differ.processMergedDiff(tree.DiffOpConvergentDelete, nil), nil
			}
			if differ.leftCurrentDiff.To == nil || differ.rightCurrentDiff.To == nil {
				return differ.processMergedDiff(tree.DiffOpDivergentDeleteConflict, nil), nil
			}
			valueCmp, err := types.CompareJSON(ctx, differ.leftCurrentDiff.To, differ.rightCurrentDiff.To)
			if err != nil {
				return ThreeWayJsonDiff{}, err
			}
			if valueCmp == 0 {
				op := tree.DiffOpConvergentModify
				if differ.leftCurrentDiff.From == nil {
					op = tree.DiffOpConvergentAdd
				}
				return differ.processMergedDiff(op, differ.leftCurrentDiff.To), nil
			}
			if differ.leftCurrentDiff.From == nil {
				return differ.processMergedDiff(tree.DiffOpDivergentModifyConflict, nil), nil
			}
			// If the key existed at base, we can do a recursive three-way merge to resolve
			// changes to the values.
			// This shouldn't be necessary: if its an object on all three branches, the original diff is recursive.
			mergedValue, conflict, err := MergeJSON(ctx, differ.ns, differ.leftCurrentDiff.From,
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
