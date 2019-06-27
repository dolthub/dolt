// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"context"
	"fmt"

	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type applyFunc func(candidate, types.ValueChanged, types.Value) candidate

func (m *merger) threeWayOrderedSequenceMerge(ctx context.Context, a, b, parent candidate, apply applyFunc, path types.Path) (types.Value, error) {
	aChangeChan, bChangeChan := make(chan types.ValueChanged), make(chan types.ValueChanged)
	aStopChan, bStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		a.diff(ctx, parent, aChangeChan, aStopChan)
		close(aChangeChan)
	}()
	go func() {
		b.diff(ctx, parent, bChangeChan, bStopChan)
		close(bChangeChan)
	}()

	defer stopAndDrain(aStopChan, aChangeChan)
	defer stopAndDrain(bStopChan, bChangeChan)

	merged := parent
	aChange, bChange := types.ValueChanged{}, types.ValueChanged{}
	for {
		// Get the next change from both a and b. If either diff(a, parent) or diff(b, parent) is complete, aChange or bChange will get an empty types.ValueChanged containing a nil Value. Generally, though, this allows us to proceed through both diffs in (key) order, considering the "current" change from both diffs at the same time.
		if aChange.Key == nil {
			aChange = <-aChangeChan
		}
		if bChange.Key == nil {
			bChange = <-bChangeChan
		}

		// Both channels are producing zero values, so we're done.
		if aChange.Key == nil && bChange.Key == nil {
			break
		}

		// Since diff generates changes in key-order, and we never skip over a change without processing it, we can simply compare the keys at which aChange and bChange occurred to determine if either is safe to apply to the merge result without further processing. This is because if, e.g. aChange.V.Less(bChange.V), we know that the diff of b will never generate a change at that key. If it was going to, it would have done so on an earlier iteration of this loop and been processed at that time.
		// It's also obviously OK to apply a change if only one diff is generating any changes, e.g. aChange.V is non-nil and bChange.V is nil.
		// TODO(binformat)
		if aChange.Key != nil && (bChange.Key == nil || aChange.Key.Less(types.Format_7_18, bChange.Key)) {
			merged = apply(merged, aChange, a.get(ctx, aChange.Key))
			aChange = types.ValueChanged{}
			continue
		} else if bChange.Key != nil && (aChange.Key == nil || bChange.Key.Less(types.Format_7_18, aChange.Key)) {
			merged = apply(merged, bChange, b.get(ctx, bChange.Key))
			bChange = types.ValueChanged{}
			continue
		}
		if !aChange.Key.Equals(bChange.Key) {
			d.Panic("Diffs have skewed!") // Sanity check.
		}

		change, mergedVal, err := m.mergeChanges(ctx, aChange, bChange, a, b, parent, apply, path)
		if err != nil {
			return parent.getValue(), err
		}
		merged = apply(merged, change, mergedVal)
		aChange, bChange = types.ValueChanged{}, types.ValueChanged{}
	}
	return merged.getValue(), nil
}

func (m *merger) mergeChanges(ctx context.Context, aChange, bChange types.ValueChanged, a, b, p candidate, apply applyFunc, path types.Path) (change types.ValueChanged, mergedVal types.Value, err error) {
	path = a.pathConcat(ctx, aChange, path)
	aValue, bValue := a.get(ctx, aChange.Key), b.get(ctx, bChange.Key)
	// If the two diffs generate different kinds of changes at the same key, conflict.
	if aChange.ChangeType != bChange.ChangeType {
		if change, mergedVal, ok := m.resolve(aChange.ChangeType, bChange.ChangeType, aValue, bValue, path); ok {
			// TODO: Correctly encode Old/NewValue with this change report. https://github.com/attic-labs/noms/issues/3467
			return types.ValueChanged{change, aChange.Key, nil, nil}, mergedVal, nil
		}
		return change, nil, newMergeConflict("Conflict:\n%s\nvs\n%s\n", describeChange(aChange), describeChange(bChange))
	}

	if aChange.ChangeType == types.DiffChangeRemoved || aValue.Equals(bValue) {
		// If both diffs generated a remove, or if the new value is the same in both, merge is fine.
		return aChange, aValue, nil
	}

	// There's one case that might still be OK even if aValue and bValue differ: different, but mergeable, compound values of the same type being added/modified at the same key, e.g. a Map being added to both a and b. If either is a primitive, or Values of different Kinds were added, though, we're in conflict.
	if !unmergeable(aValue, bValue) {
		// TODO: Add concurrency.
		var err error
		if mergedVal, err = m.threeWay(ctx, aValue, bValue, p.get(ctx, aChange.Key), path); err == nil {
			return aChange, mergedVal, nil
		}
		return change, nil, err
	}

	if change, mergedVal, ok := m.resolve(aChange.ChangeType, bChange.ChangeType, aValue, bValue, path); ok {
		// TODO: Correctly encode Old/NewValue with this change report. https://github.com/attic-labs/noms/issues/3467
		return types.ValueChanged{change, aChange.Key, nil, nil}, mergedVal, nil
	}
	return change, nil, newMergeConflict("Conflict:\n%s = %s\nvs\n%s = %s", describeChange(aChange), types.EncodedValue(ctx, aValue), describeChange(bChange), types.EncodedValue(ctx, bValue))
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
	return fmt.Sprintf("%s %s", op, types.EncodedValue(context.Background(), change.Key))
}
