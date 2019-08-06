// Copyright 2019 Liquidata, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"context"
	"fmt"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"

	"github.com/liquidata-inc/dolt/go/store/d"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type applyFunc func(candidate, types.ValueChanged, types.Value) (candidate, error)

func (m *merger) threeWayOrderedSequenceMerge(ctx context.Context, a, b, parent candidate, apply applyFunc, path types.Path) (types.Value, error) {
	ae := atomicerr.New()
	aChangeChan, bChangeChan := make(chan types.ValueChanged), make(chan types.ValueChanged)
	aStopChan, bStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		defer close(aChangeChan)
		a.diff(ctx, parent, ae, aChangeChan, aStopChan)
	}()
	go func() {
		defer close(bChangeChan)
		b.diff(ctx, parent, ae, bChangeChan, bStopChan)
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
		if aChange.Key != nil {
			var err error
			noBOrALessB := bChange.Key == nil
			if !noBOrALessB {
				noBOrALessB, err = aChange.Key.Less(m.vrw.Format(), bChange.Key)

				if err != nil {
					return nil, err
				}
			}

			if noBOrALessB {
				v, _, err := a.get(ctx, aChange.Key)

				if err != nil {
					return nil, err
				}

				merged, err = apply(merged, aChange, v)

				if err != nil {
					return nil, err
				}

				aChange = types.ValueChanged{}
				continue
			}
		}

		if bChange.Key != nil {
			var err error
			noAOrBLessA := aChange.Key == nil

			if !noAOrBLessA {
				noAOrBLessA, err = bChange.Key.Less(m.vrw.Format(), aChange.Key)

				if err != nil {
					return nil, err
				}
			}

			if noAOrBLessA {
				v, _, err := b.get(ctx, bChange.Key)

				if err != nil {
					return nil, err
				}

				merged, err = apply(merged, bChange, v)

				if err != nil {
					return nil, err
				}

				bChange = types.ValueChanged{}
				continue
			}
		}

		if !aChange.Key.Equals(bChange.Key) {
			d.Panic("Diffs have skewed!") // Sanity check.
		}

		change, mergedVal, err := m.mergeChanges(ctx, aChange, bChange, a, b, parent, apply, path)
		if err != nil {
			return parent.getValue(), err
		}
		merged, err = apply(merged, change, mergedVal)

		if err != nil {
			return nil, err
		}

		aChange, bChange = types.ValueChanged{}, types.ValueChanged{}
	}
	return merged.getValue(), nil
}

func (m *merger) mergeChanges(ctx context.Context, aChange, bChange types.ValueChanged, a, b, p candidate, apply applyFunc, path types.Path) (change types.ValueChanged, mergedVal types.Value, err error) {
	path, err = a.pathConcat(ctx, aChange, path)

	if err != nil {
		return types.ValueChanged{}, nil, err
	}

	aValue, _, err := a.get(ctx, aChange.Key)

	if err != nil {
		return types.ValueChanged{}, nil, err
	}

	bValue, _, err := b.get(ctx, bChange.Key)

	if err != nil {
		return types.ValueChanged{}, nil, err
	}

	// If the two diffs generate different kinds of changes at the same key, conflict.
	if aChange.ChangeType != bChange.ChangeType {
		if change, mergedVal, ok := m.resolve(aChange.ChangeType, bChange.ChangeType, aValue, bValue, path); ok {
			// TODO: Correctly encode Old/NewValue with this change report. https://github.com/attic-labs/noms/issues/3467
			return types.ValueChanged{ChangeType: change, Key: aChange.Key, OldValue: nil, NewValue: nil}, mergedVal, nil
		}

		aDesc, err := describeChange(aChange)

		if err != nil {
			return types.ValueChanged{}, nil, err
		}

		bDesc, err := describeChange(bChange)

		if err != nil {
			return types.ValueChanged{}, nil, err
		}

		return change, nil, newMergeConflict("Conflict:\n%s\nvs\n%s\n", aDesc, bDesc)
	}

	if aChange.ChangeType == types.DiffChangeRemoved || aValue.Equals(bValue) {
		// If both diffs generated a remove, or if the new value is the same in both, merge is fine.
		return aChange, aValue, nil
	}

	// There's one case that might still be OK even if aValue and bValue differ: different, but mergeable, compound values of the same type being added/modified at the same key, e.g. a Map being added to both a and b. If either is a primitive, or Values of different Kinds were added, though, we're in conflict.
	if !unmergeable(aValue, bValue) {
		v, _, err := p.get(ctx, aChange.Key)

		if err != nil {
			return types.ValueChanged{}, nil, err
		}

		// TODO: Add concurrency.
		if mergedVal, err = m.threeWay(ctx, aValue, bValue, v, path); err == nil {
			return aChange, mergedVal, nil
		}
		return change, nil, err
	}

	if change, mergedVal, ok := m.resolve(aChange.ChangeType, bChange.ChangeType, aValue, bValue, path); ok {
		// TODO: Correctly encode Old/NewValue with this change report. https://github.com/attic-labs/noms/issues/3467
		return types.ValueChanged{ChangeType: change, Key: aChange.Key, OldValue: nil, NewValue: nil}, mergedVal, nil
	}

	aStr, err := types.EncodedValue(ctx, aValue)

	if err != nil {
		return types.ValueChanged{}, nil, err
	}

	aDesc, err := describeChange(aChange)

	if err != nil {
		return types.ValueChanged{}, nil, err
	}

	bStr, err := types.EncodedValue(ctx, bValue)

	if err != nil {
		return types.ValueChanged{}, nil, err
	}

	bDesc, err := describeChange(bChange)

	if err != nil {
		return types.ValueChanged{}, nil, err
	}

	return change, nil, newMergeConflict("Conflict:\n%s = %s\nvs\n%s = %s", aDesc, aStr, bDesc, bStr)
}

func stopAndDrain(stop chan<- struct{}, drain <-chan types.ValueChanged) {
	close(stop)
	for range drain {
	}
}

func describeChange(change types.ValueChanged) (string, error) {
	op := ""
	switch change.ChangeType {
	case types.DiffChangeAdded:
		op = "added"
	case types.DiffChangeModified:
		op = "modded"
	case types.DiffChangeRemoved:
		op = "removed"
	}

	str, err := types.EncodedValue(context.Background(), change.Key)

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s %s", op, str), nil
}
