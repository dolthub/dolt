// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package merge

import (
	"fmt"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
)

// ResolveFunc is the type for custom merge-conflict resolution callbacks.
// When the merge algorithm encounters two non-mergeable changes (aChange and
// bChange) at the same path, it calls the ResolveFunc passed into ThreeWay().
// The callback gets the types of the two incompatible changes (added, changed
// or removed) and the two Values that could not be merged (if any). If the
// ResolveFunc cannot devise a resolution, ok should be false upon return and
// the other return values are undefined. If the conflict can be resolved, the
// function should return the appropriate type of change to apply, the new value
// to be used (if any), and true.
type ResolveFunc func(aChange, bChange types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool)

// ErrMergeConflict indicates that a merge attempt failed and must be resolved
// manually for the provided reason.
type ErrMergeConflict struct {
	msg string
}

func (e *ErrMergeConflict) Error() string {
	return e.msg
}

func newMergeConflict(format string, args ...interface{}) *ErrMergeConflict {
	return &ErrMergeConflict{fmt.Sprintf(format, args...)}
}

// ThreeWay attempts a three-way merge between two candidates and a common
// ancestor.
// It considers the three of them recursively, applying some simple rules to
// identify conflicts:
//  - If any of the three nodes are different NomsKinds
//  - If we are dealing with a map:
//    - same key is both removed and inserted wrt parent: conflict
//    - same key is inserted wrt parent, but with different values: conflict
//  - If we are dealing with a struct:
//    - same field is both removed and inserted wrt parent: conflict
//    - same field is inserted wrt parent, but with different values: conflict
//  - If we are dealing with a list:
//    - same index is both removed and inserted wrt parent: conflict
//    - same index is inserted wrt parent, but with different values: conflict
//  - If we are dealing with a set:
//    - `merged` is essentially union(a, b, parent)
//
// All other modifications are allowed.
// ThreeWay() works on types.List, types.Map, types.Set, and types.Struct.
func ThreeWay(a, b, parent types.Value, vrw types.ValueReadWriter, resolve ResolveFunc, progress chan struct{}) (merged types.Value, err error) {
	describe := func(v types.Value) string {
		if v != nil {
			return v.Type().Describe()
		}
		return "nil Value"
	}

	if a == nil && b == nil {
		return parent, nil
	} else if unmergeable(a, b) {
		return parent, newMergeConflict("Cannot merge %s with %s.", describe(a), describe(b))
	}

	if resolve == nil {
		resolve = defaultResolve
	}
	m := &merger{vrw, resolve, progress}
	return m.threeWay(a, b, parent, types.Path{})
}

// a and b cannot be merged if they are of different NomsKind, or if at least one of the two is nil, or if either is a Noms primitive.
func unmergeable(a, b types.Value) bool {
	if a != nil && b != nil {
		aKind, bKind := a.Type().Kind(), b.Type().Kind()
		return aKind != bKind || types.IsPrimitiveKind(aKind) || types.IsPrimitiveKind(bKind)
	}
	return true
}

type merger struct {
	vrw      types.ValueReadWriter
	resolve  ResolveFunc
	progress chan<- struct{}
}

func defaultResolve(aChange, bChange types.DiffChangeType, a, b types.Value, p types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
	return
}

func updateProgress(progress chan<- struct{}) {
	// TODO: Eventually we'll want more information than a single bit :).
	if progress != nil {
		progress <- struct{}{}
	}
}

func (m *merger) threeWay(a, b, parent types.Value, path types.Path) (merged types.Value, err error) {
	defer updateProgress(m.progress)
	d.PanicIfTrue(a == nil || b == nil, "Merge candidates cannont be nil: a = %v, b = %v", a, b)

	switch a.Type().Kind() {
	case types.ListKind:
		if aList, bList, pList, ok := listAssert(a, b, parent); ok {
			return threeWayListMerge(aList, bList, pList)
		}

	case types.MapKind:
		if aMap, bMap, pMap, ok := mapAssert(a, b, parent); ok {
			return m.threeWayMapMerge(aMap, bMap, pMap, path)
		}

	case types.RefKind:
		if aValue, bValue, pValue, ok := refAssert(a, b, parent, m.vrw); ok {
			merged, err := m.threeWay(aValue, bValue, pValue, path)
			if err != nil {
				return parent, err
			}
			return m.vrw.WriteValue(merged), nil
		}

	case types.SetKind:
		if aSet, bSet, pSet, ok := setAssert(a, b, parent); ok {
			return m.threeWaySetMerge(aSet, bSet, pSet, path)
		}

	case types.StructKind:
		if aStruct, bStruct, pStruct, ok := structAssert(a, b, parent); ok {
			return m.threeWayStructMerge(aStruct, bStruct, pStruct, path)
		}
	}

	pDescription := "<nil>"
	if parent != nil {
		pDescription = parent.Type().Describe()
	}
	return parent, newMergeConflict("Cannot merge %s and %s on top of %s.", a.Type().Describe(), b.Type().Describe(), pDescription)
}

func (m *merger) threeWayMapMerge(a, b, parent types.Map, path types.Path) (merged types.Value, err error) {
	apply := func(target candidate, change types.ValueChanged, newVal types.Value) candidate {
		defer updateProgress(m.progress)
		switch change.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			return mapCandidate{target.getValue().(types.Map).Set(change.V, newVal)}
		case types.DiffChangeRemoved:
			return mapCandidate{target.getValue().(types.Map).Remove(change.V)}
		default:
			panic("Not Reached")
		}
	}
	return m.threeWayOrderedSequenceMerge(mapCandidate{a}, mapCandidate{b}, mapCandidate{parent}, apply, path)
}

func (m *merger) threeWaySetMerge(a, b, parent types.Set, path types.Path) (merged types.Value, err error) {
	apply := func(target candidate, change types.ValueChanged, newVal types.Value) candidate {
		defer updateProgress(m.progress)
		switch change.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			return setCandidate{target.getValue().(types.Set).Insert(newVal)}
		case types.DiffChangeRemoved:
			return setCandidate{target.getValue().(types.Set).Remove(newVal)}
		default:
			panic("Not Reached")
		}
	}
	return m.threeWayOrderedSequenceMerge(setCandidate{a}, setCandidate{b}, setCandidate{parent}, apply, path)
}

func (m *merger) threeWayStructMerge(a, b, parent types.Struct, path types.Path) (merged types.Value, err error) {
	apply := func(target candidate, change types.ValueChanged, newVal types.Value) candidate {
		defer updateProgress(m.progress)
		// Right now, this always iterates over all fields to create a new Struct, because there's no API for adding/removing a field from an existing struct type.
		targetVal := target.getValue().(types.Struct)
		if f, ok := change.V.(types.String); ok {
			field := string(f)
			data := types.StructData{}
			desc := targetVal.Type().Desc.(types.StructDesc)
			desc.IterFields(func(name string, t *types.Type) {
				if name != field {
					data[name] = targetVal.Get(name)
				}
			})
			if change.ChangeType == types.DiffChangeAdded || change.ChangeType == types.DiffChangeModified {
				data[field] = newVal
			}
			return structCandidate{types.NewStruct(desc.Name, data)}
		}
		panic(fmt.Errorf("Bad key type in diff: %s", change.V.Type().Describe()))
	}
	return m.threeWayOrderedSequenceMerge(structCandidate{a}, structCandidate{b}, structCandidate{parent}, apply, path)
}

func listAssert(a, b, parent types.Value) (aList, bList, pList types.List, ok bool) {
	var aOk, bOk, pOk bool
	aList, aOk = a.(types.List)
	bList, bOk = b.(types.List)
	if parent != nil {
		pList, pOk = parent.(types.List)
	} else {
		pList, pOk = types.NewList(), true
	}
	return aList, bList, pList, aOk && bOk && pOk
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
	return aMap, bMap, pMap, aOk && bOk && pOk
}

func refAssert(a, b, parent types.Value, vrw types.ValueReadWriter) (aValue, bValue, pValue types.Value, ok bool) {
	var aOk, bOk, pOk bool
	var aRef, bRef, pRef types.Ref
	aRef, aOk = a.(types.Ref)
	bRef, bOk = b.(types.Ref)
	if !aOk || !bOk {
		return
	}

	aValue = aRef.TargetValue(vrw)
	bValue = bRef.TargetValue(vrw)
	if parent != nil {
		if pRef, pOk = parent.(types.Ref); pOk {
			pValue = pRef.TargetValue(vrw)
		}
	} else {
		pOk = true // parent == nil is still OK. It just leaves pValue as nil.
	}
	return aValue, bValue, pValue, aOk && bOk && pOk
}

func setAssert(a, b, parent types.Value) (aSet, bSet, pSet types.Set, ok bool) {
	var aOk, bOk, pOk bool
	aSet, aOk = a.(types.Set)
	bSet, bOk = b.(types.Set)
	if parent != nil {
		pSet, pOk = parent.(types.Set)
	} else {
		pSet, pOk = types.NewSet(), true
	}
	return aSet, bSet, pSet, aOk && bOk && pOk
}

func structAssert(a, b, parent types.Value) (aStruct, bStruct, pStruct types.Struct, ok bool) {
	var aOk, bOk, pOk bool
	aStruct, aOk = a.(types.Struct)
	bStruct, bOk = b.(types.Struct)
	if aOk && bOk {
		aDesc, bDesc := a.Type().Desc.(types.StructDesc), b.Type().Desc.(types.StructDesc)
		if aDesc.Name == bDesc.Name {
			if parent != nil {
				pStruct, pOk = parent.(types.Struct)
			} else {
				pStruct, pOk = types.NewStruct(aDesc.Name, nil), true
			}
			return aStruct, bStruct, pStruct, pOk
		}
	}
	return
}
