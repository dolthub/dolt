// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// ErrMergeConflict indicates that a merge attempt failed and must be resolved manually for the provided reason.
type ErrMergeConflict struct {
	msg string
}

func (e *ErrMergeConflict) Error() string {
	return e.msg
}

func newMergeConflict(format string, args ...interface{}) *ErrMergeConflict {
	return &ErrMergeConflict{fmt.Sprintf(format, args...)}
}

// ThreeWay attempts a three-way merge between two candidates and a common ancestor. It considers the three of them recursively, applying some simple rules to identify conflicts:
//  - If any of the three nodes are different NomsKinds: conflict
//  - If we are dealing with a map:
//    - If the same key is both removed and inserted wrt parent: conflict
//    - If the same key is inserted wrt parent, but with different values: conflict
//  - If we are dealing with a struct:
//    - If the same field is both removed and inserted wrt parent: conflict
//    - If the same field is inserted wrt parent, but with different values: conflict
//  - If we are dealing with a list:
//    - If the same index is both removed and inserted wrt parent: conflict
//    - If the same index is inserted wrt parent, but with different values: conflict
//  - If we are dealing with a set:
//    - If the same object is both removed and inserted wrt parent: conflict
//
// All other modifications are allowed.
// Currently, ThreeWay() only works on types.Map.
func ThreeWay(a, b, parent types.Value, vwr types.ValueReadWriter) (merged types.Value, err error) {
	if a == nil && b == nil {
		return parent, nil
	} else if a == nil {
		return parent, newMergeConflict("Cannot merge nil Value with %s.", b.Type().Describe())
	} else if b == nil {
		return parent, newMergeConflict("Cannot merge %s with nil value.", a.Type().Describe())
	} else if unmergeable(a, b) {
		return parent, newMergeConflict("Cannot merge %s with %s.", a.Type().Describe(), b.Type().Describe())
	}

	return threeWayMerge(a, b, parent, vwr)
}

// a and b cannot be merged if they are of different NomsKind, or if at least one of the two is nil, or if either is a Noms primitive.
func unmergeable(a, b types.Value) bool {
	if a != nil && b != nil {
		aKind, bKind := a.Type().Kind(), b.Type().Kind()
		return aKind != bKind || types.IsPrimitiveKind(aKind) || types.IsPrimitiveKind(bKind)
	}
	return true
}

func threeWayMerge(a, b, parent types.Value, vwr types.ValueReadWriter) (merged types.Value, err error) {
	d.PanicIfTrue(a == nil || b == nil, "Merge candidates cannont be nil: a = %v, b = %v", a, b)
	newTypeConflict := func() *ErrMergeConflict {
		pDescription := "<nil>"
		if parent != nil {
			pDescription = parent.Type().Describe()
		}
		return newMergeConflict("Cannot merge %s and %s on top of %s.", a.Type().Describe(), b.Type().Describe(), pDescription)
	}

	switch a.Type().Kind() {
	case types.ListKind:
		// TODO: Come up with a plan for List (BUG 148)
		return parent, newMergeConflict("Cannot merge %s.", a.Type().Describe())

	case types.MapKind:
		if aMap, bMap, pMap, ok := mapAssert(a, b, parent); ok {
			return threeWayMapMerge(aMap, bMap, pMap, vwr)
		}

	case types.RefKind:
		if aValue, bValue, pValue, ok := refAssert(a, b, parent, vwr); ok {
			merged, err := threeWayMerge(aValue, bValue, pValue, vwr)
			if err != nil {
				return parent, err
			}
			return vwr.WriteValue(merged), nil
		}

	case types.SetKind:
		// TODO: Implement plan from BUG148
		return parent, newMergeConflict("Cannot merge %s.", a.Type().Describe())

	case types.StructKind:
		if aStruct, bStruct, pStruct, ok := structAssert(a, b, parent); ok {
			return threeWayStructMerge(aStruct, bStruct, pStruct, vwr)
		}

	default:
		return parent, newMergeConflict("Cannot merge %s.", a.Type().Describe())

	}
	return parent, newTypeConflict()
}

func threeWayMapMerge(a, b, parent types.Map, vwr types.ValueReadWriter) (merged types.Map, err error) {
	aChangeChan, bChangeChan := make(chan types.ValueChanged), make(chan types.ValueChanged)
	aStopChan, bStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		a.DiffLeftRight(parent, aChangeChan, aStopChan)
		close(aChangeChan)
	}()
	go func() {
		b.DiffLeftRight(parent, bChangeChan, bStopChan)
		close(bChangeChan)
	}()
	stopAndDrain := func(stop chan<- struct{}, drain <-chan types.ValueChanged) {
		close(stop)
		for range drain {
		}
	}
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
			merged = apply(merged, aChange, a.Get(aChange.V))
			aChange = types.ValueChanged{}
			continue
		} else if bChange.V != nil && (aChange.V == nil || bChange.V.Less(aChange.V)) {
			merged = apply(merged, bChange, b.Get(bChange.V))
			bChange = types.ValueChanged{}
			continue
		}
		d.PanicIfTrue(!aChange.V.Equals(bChange.V), "Diffs have skewed!") // Sanity check.

		// If the two diffs generate different kinds of changes at the same key, conflict.
		if aChange.ChangeType != bChange.ChangeType {
			return parent, newMergeConflict("Conflict:\n%s\nvs\n%s\n", describeChange(aChange), describeChange(bChange))
		}

		aValue, bValue := a.Get(aChange.V), b.Get(bChange.V)
		if aChange.ChangeType == types.DiffChangeRemoved || aValue.Equals(bValue) {
			// If both diffs generated a remove, or if the new value is the same in both, merge is fine.
			merged = apply(merged, aChange, aValue)
		} else {
			// There's one case that might still be OK even if aValue and bValue differ: different, but mergeable, compound values of the same type being added/modified at the same key, e.g. a Map being added to both a and b. If either is a primitive, or Values of different Kinds were added, though, we're in conflict.
			if unmergeable(aValue, bValue) {
				return parent, newMergeConflict("Conflict:\n%s = %s\nvs\n%s = %s", describeChange(aChange), types.EncodedValue(aValue), describeChange(bChange), types.EncodedValue(bValue))
			}
			// TODO: Add concurrency.
			mergedValue, err := threeWayMerge(aValue, bValue, parent.Get(aChange.V), vwr)
			if err != nil {
				return parent, err
			}
			merged = merged.Set(aChange.V, mergedValue)
		}
		aChange, bChange = types.ValueChanged{}, types.ValueChanged{}
	}
	return merged, nil
}

func apply(target types.Map, change types.ValueChanged, newVal types.Value) types.Map {
	switch change.ChangeType {
	case types.DiffChangeAdded, types.DiffChangeModified:
		return target.Set(change.V, newVal)
	case types.DiffChangeRemoved:
		return target.Remove(change.V)
	default:
		panic("Not Reached")
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

func threeWayStructMerge(a, b, parent types.Struct, vwr types.ValueReadWriter) (merged types.Struct, err error) {
	return parent, newMergeConflict("Cannot merge %s.", a.Type().Describe())
}

func mapAssert(a, b, parent types.Value) (aMap, bMap, pMap types.Map, ok bool) {
	var aOk, bOk, pOk bool
	aMap, aOk = a.(types.Map)
	bMap, bOk = b.(types.Map)
	if parent != nil {
		pMap, pOk = parent.(types.Map)
	} else {
		pMap, pOk = types.NewMap(), true
	}
	ok = aOk && bOk && pOk
	return
}

func refAssert(a, b, parent types.Value, vwr types.ValueReadWriter) (aValue, bValue, pValue types.Value, ok bool) {
	var aOk, bOk, pOk bool
	var aRef, bRef, pRef types.Ref
	aRef, aOk = a.(types.Ref)
	bRef, bOk = b.(types.Ref)
	if !aOk || !bOk {
		return
	}

	aValue = aRef.TargetValue(vwr)
	bValue = bRef.TargetValue(vwr)
	if parent != nil {
		if pRef, pOk = parent.(types.Ref); pOk {
			pValue = pRef.TargetValue(vwr)
		}
	} else {
		pOk = true // parent == nil is still OK. It just leaves pValue as nil.
	}
	return aValue, bValue, pValue, aOk && bOk && pOk
}

func structAssert(a, b, parent types.Value) (aStruct, bStruct, pStruct types.Struct, ok bool) {
	var aOk, bOk, pOk bool
	aStruct, aOk = a.(types.Struct)
	bStruct, bOk = b.(types.Struct)
	if parent != nil {
		pStruct, pOk = parent.(types.Struct)
	} else {
		pStruct, pOk = aStruct, true
	}
	ok = aOk && bOk && pOk && aStruct.Type().Equals(bStruct.Type()) && aStruct.Type().Equals(pStruct.Type())
	return
}
