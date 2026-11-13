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
			leftStillInMatchedArray, err := tree.EqualJsonDiffKeys(leftArray, differ.lastMatchedArrayPath)
			if err != nil {
				return ThreeWayJsonDiff{}, err
			}
			rightStillInMatchedArray, err := tree.EqualJsonDiffKeys(rightArray, differ.lastMatchedArrayPath)
			if err != nil {
				return ThreeWayJsonDiff{}, err
			}
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

		cmp, err := tree.CompareJsonDiffKeys(leftKey, rightKey)
		if err != nil {
			return ThreeWayJsonDiff{}, err
		}
		if cmp != 0 {
			sameArray, err := tree.EqualJsonDiffKeys(leftArray, rightArray)
			if err != nil {
				return ThreeWayJsonDiff{}, err
			}
			if leftArray != nil && sameArray {
				return ThreeWayJsonDiff{Op: tree.DiffOpDivergentModifyConflict}, nil
			}
			// Check for a replacement or removal that overlaps a descendant edit.
			earlier, later := leftKey, rightKey
			if cmp > 0 {
				earlier, later = rightKey, leftKey
			}
			overlap, err := tree.IsJsonKeyPrefix(later, earlier)
			if err != nil {
				return ThreeWayJsonDiff{}, err
			}
			if overlap {
				return ThreeWayJsonDiff{Op: tree.DiffOpDivergentModifyConflict}, nil
			}
			if cmp > 0 {
				return differ.processRightSideOnlyDiff(), nil
			}
			// A left-only edit is already in the document.
			differ.leftCurrentDiff = nil
			continue
		}

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
