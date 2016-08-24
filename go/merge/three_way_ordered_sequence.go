// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

type diffFunc func(chan<- types.ValueChanged, <-chan struct{})
type applyFunc func(types.Value, types.ValueChanged, types.Value) types.Value
type getFunc func(types.Value) types.Value

func threeWayOrderedSequenceMerge(parent types.Value, aDiff, bDiff diffFunc, aGet, bGet, pGet getFunc, apply applyFunc, vwr types.ValueReadWriter) (merged types.Value, err error) {
	aChangeChan, bChangeChan := make(chan types.ValueChanged), make(chan types.ValueChanged)
	aStopChan, bStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		aDiff(aChangeChan, aStopChan)
		close(aChangeChan)
	}()
	go func() {
		bDiff(bChangeChan, bStopChan)
		close(bChangeChan)
	}()

	defer stopAndDrain(aStopChan, aChangeChan)
	defer stopAndDrain(bStopChan, bChangeChan)

	merged = parent
	aChange, bChange := types.ValueChanged{}, types.ValueChanged{}
	for {
		// Get the next change from both a and b. If either diff(a, parent) or diff(b, parent) is complete, aChange or bChange will get an empty types.ValueChanged containing a nil Value. Generally, though, this allows us to proceed through both diffs in (key) order, considering the "current" change from both diffs at the same time.
		if aChange.V == nil {
			aChange = <-aChangeChan
		}
		if bChange.V == nil {
			bChange = <-bChangeChan
		}

		// Both channels are producing zero values, so we're done.
		if aChange.V == nil && bChange.V == nil {
			break
		}

		// Since diff generates changes in key-order, and we never skip over a change without processing it, we can simply compare the keys at which aChange and bChange occurred to determine if either is safe to apply to the merge result without further processing. This is because if, e.g. aChange.V.Less(bChange.V), we know that the diff of b will never generate a change at that key. If it was going to, it would have done so on an earlier iteration of this loop and been processed at that time.
		// It's also obviously OK to apply a change if only one diff is generating any changes, e.g. aChange.V is non-nil and bChange.V is nil.
		if aChange.V != nil && (bChange.V == nil || aChange.V.Less(bChange.V)) {
			merged = apply(merged, aChange, aGet(aChange.V))
			aChange = types.ValueChanged{}
			continue
		} else if bChange.V != nil && (aChange.V == nil || bChange.V.Less(aChange.V)) {
			merged = apply(merged, bChange, bGet(bChange.V))
			bChange = types.ValueChanged{}
			continue
		}
		d.PanicIfTrue(!aChange.V.Equals(bChange.V), "Diffs have skewed!") // Sanity check.

		// If the two diffs generate different kinds of changes at the same key, conflict.
		if aChange.ChangeType != bChange.ChangeType {
			return parent, newMergeConflict("Conflict:\n%s\nvs\n%s\n", describeChange(aChange), describeChange(bChange))
		}

		aValue, bValue := aGet(aChange.V), bGet(bChange.V)
		if aChange.ChangeType == types.DiffChangeRemoved || aValue.Equals(bValue) {
			// If both diffs generated a remove, or if the new value is the same in both, merge is fine.
			merged = apply(merged, aChange, aValue)
		} else {
			// There's one case that might still be OK even if aValue and bValue differ: different, but mergeable, compound values of the same type being added/modified at the same key, e.g. a Map being added to both a and b. If either is a primitive, or Values of different Kinds were added, though, we're in conflict.
			if unmergeable(aValue, bValue) {
				return parent, newMergeConflict("Conflict:\n%s = %s\nvs\n%s = %s", describeChange(aChange), types.EncodedValue(aValue), describeChange(bChange), types.EncodedValue(bValue))
			}
			// TODO: Add concurrency.
			mergedValue, err := threeWayMerge(aValue, bValue, pGet(aChange.V), vwr)
			if err != nil {
				return parent, err
			}
			merged = apply(merged, aChange, mergedValue)
		}
		aChange, bChange = types.ValueChanged{}, types.ValueChanged{}
	}
	return merged, nil
}

func stopAndDrain(stop chan<- struct{}, drain <-chan types.ValueChanged) {
	close(stop)
	for range drain {
	}
}

func describeChange(change types.ValueChanged) string {
	op := ""
	switch change.ChangeType {
	case types.DiffChangeAdded:
		op = "added"
	case types.DiffChangeModified:
		op = "modded"
	case types.DiffChangeRemoved:
		op = "removed"
	}
	return fmt.Sprintf("%s %s", op, types.EncodedValue(change.V))
}
