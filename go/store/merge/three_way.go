// Copyright 2019 Dolthub, Inc.
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
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
)

// Policy functors are used to merge two values (a and b) against a common
// ancestor. All three Values and their must by wholly readable from vrw.
// Whenever a change is merged, implementations should send a struct{} over
// progress.
type Policy func(ctx context.Context, a, b, ancestor types.Value, vrw types.ValueReadWriter, progress chan struct{}) (merged types.Value, err error)

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

// None is the no-op ResolveFunc. Any conflict results in a merge failure.
func None(aChange, bChange types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
	return change, merged, false
}

// Ours resolves conflicts by preferring changes from the Value currently being comitted.
func Ours(aChange, bChange types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
	return aChange, a, true
}

// Theirs resolves conflicts by preferring changes in the current HEAD.
func Theirs(aChange, bChange types.DiffChangeType, a, b types.Value, path types.Path) (change types.DiffChangeType, merged types.Value, ok bool) {
	return bChange, b, true
}

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

// NewThreeWay creates a new Policy based on ThreeWay using the provided
// ResolveFunc.
func NewThreeWay(resolve ResolveFunc) Policy {
	return func(ctx context.Context, a, b, parent types.Value, vrw types.ValueReadWriter, progress chan struct{}) (merged types.Value, err error) {
		return ThreeWay(ctx, a, b, parent, vrw, resolve, progress)
	}
}

// ThreeWay attempts a three-way merge between two _candidate_ values that
// have both changed with respect to a common _parent_ value. The result of
// the algorithm is a _merged_ value or an error if merging could not be done.
//
// The algorithm works recursively, applying the following rules for each value:
//
// - If any of the three values have a different [kind](link): conflict
// - If the two candidates are identical: the result is that value
// - If the values are primitives or Blob: conflict
// - If the values are maps:
//   - if the same key was inserted or updated in both candidates:
//   - first run this same algorithm on those two values to attempt to merge them
//   - if the two merged values are still different: conflict
//   - if a key was inserted in one candidate and removed in the other: conflict
//
// - If the values are structs:
//   - Same as map, except using field names instead of map keys
//
// - If the values are sets:
//   - Apply the changes from both candidates to the parent to get the result. No conflicts are possible.
//
// - If the values are list:
//   - Apply list-merge (see below)
//
// Merge rules for List are a bit more complex than Map, Struct, and Set due
// to a wider away of potential use patterns. A List might be a de-facto Map
// with sequential numeric keys, or it might be a sequence of objects where
// order matters but the caller is unlikely to go back and update the value at
// a given index. List modifications are expressed in terms of 'splices' (see
// types/edit_distance.go). Roughly, a splice indicates that some number of
// elements were added and/or removed at some index in |parent|. In the
// following example:
//
// parent: [a, b, c, d]
// a:      [b, c, d]
// b:      [a, b, c, d, e]
// merged: [b, c, d, e]
//
// The difference from parent -> is described by the splice {0, 1}, indicating
// that 1 element was removed from parent at index 0. The difference from
// parent -> b is described as {4, 0, e}, indicating that 0 elements were
// removed at parent's index 4, and the element 'e' was added. Our merge
// algorithm will successfully merge a and b, because these splices do not
// overlap; that is, neither one removes the index at which the other
// operates. As a general rule, the merge algorithm will refuse to merge
// splices that overlap, as in the following examples:
//
// parent: [a, b, c]
// a:      [a, d, b, c]
// b:      [a, c]
// merged: conflict
//
// parent: [a, b, c]
// a:      [a, e, b, c]
// b:      [a, d, b, c]
// merged: conflict
//
// The splices in the first example are {1, 0, d} (remove 0 elements at index
// 1 and add 'd') and {1, 1} (remove 1 element at index 1). Since the latter
// removes the element at which the former adds an element, these splices
// overlap. Similarly, in the second example, both splices operate at index 1
// but add different elements. Thus, they also overlap.
//
// There is one special case for overlapping splices. If they perform the
// exact same operation, the algorithm considers them not to be in conflict.
// E.g.
//
// parent: [a, b, c]
// a:      [a, d, e]
// b:      [a, d, e]
// merged: [a, d, e]
func ThreeWay(ctx context.Context, a, b, parent types.Value, vrw types.ValueReadWriter, resolve ResolveFunc, progress chan struct{}) (merged types.Value, err error) {
	describe := func(v types.Value) (string, error) {
		if v != nil {
			t, err := types.TypeOf(v)

			if err != nil {
				return "", err
			}

			str, err := t.Describe(ctx)

			if err != nil {
				return "", err
			}

			return str, nil
		}

		return "nil Value", nil
	}

	if a == nil && b == nil {
		return parent, nil
	} else if unmergeable(a, b) {
		aDesc, err := describe(a)

		if err != nil {
			return nil, err
		}

		bDesc, err := describe(b)

		if err != nil {
			return nil, err
		}

		return parent, newMergeConflict("Cannot merge %s with %s.", aDesc, bDesc)
	}

	if resolve == nil {
		resolve = None
	}
	m := &merger{vrw, resolve, progress}
	return m.threeWay(ctx, a, b, parent, types.Path{})
}

// a and b cannot be merged if they are of different NomsKind, or if at least one of the two is nil, or if either is a Noms primitive.
func unmergeable(a, b types.Value) bool {
	if a != nil && b != nil {
		aKind, bKind := a.Kind(), b.Kind()
		return aKind != bKind || types.IsPrimitiveKind(aKind) || types.IsPrimitiveKind(bKind)
	}
	return true
}

type merger struct {
	vrw      types.ValueReadWriter
	resolve  ResolveFunc
	progress chan<- struct{}
}

func updateProgress(progress chan<- struct{}) {
	// TODO: Eventually we'll want more information than a single bit :).
	if progress != nil {
		progress <- struct{}{}
	}
}

func (m *merger) threeWay(ctx context.Context, a, b, parent types.Value, path types.Path) (merged types.Value, err error) {
	defer updateProgress(m.progress)

	if a == nil || b == nil {
		d.Panic("Merge candidates cannot be nil: a = %v, b = %v", a, b)
	}

	switch a.Kind() {
	case types.ListKind:
		if aList, bList, pList, ok, err := listAssert(ctx, m.vrw, a, b, parent); err != nil {
			return nil, err
		} else if ok {
			return threeWayListMerge(ctx, aList, bList, pList)
		}

	case types.MapKind:
		if aMap, bMap, pMap, ok, err := mapAssert(ctx, m.vrw, a, b, parent); err != nil {
			return nil, err
		} else if ok {
			return m.threeWayMapMerge(ctx, aMap, bMap, pMap, path)
		}

	case types.RefKind:
		if aValue, bValue, pValue, ok, err := refAssert(ctx, a, b, parent, m.vrw); err != nil {
			return nil, err
		} else if ok {
			merged, err := m.threeWay(ctx, aValue, bValue, pValue, path)
			if err != nil {
				return parent, err
			}
			return m.vrw.WriteValue(ctx, merged)
		}

	case types.SetKind:
		if aSet, bSet, pSet, ok, err := setAssert(ctx, m.vrw, a, b, parent); err != nil {
			return nil, err

		} else if ok {
			return m.threeWaySetMerge(ctx, aSet, bSet, pSet, path)
		}

	case types.StructKind:
		if aStruct, bStruct, pStruct, ok, err := structAssert(a, b, parent); err != nil {
			return nil, err
		} else if ok {
			return m.threeWayStructMerge(ctx, aStruct, bStruct, pStruct, path)
		}
	}

	pDescription := "<nil>"
	if parent != nil {
		t, err := types.TypeOf(parent)

		if err != nil {
			return nil, err
		}

		pDescription, err = t.Describe(ctx)

		if err != nil {
			return nil, err
		}
	}

	aType, err := types.TypeOf(a)
	bType, err := types.TypeOf(b)
	aDesc, err := aType.Describe(ctx)
	bDesc, err := bType.Describe(ctx)

	return parent, newMergeConflict("Cannot merge %s and %s on top of %s.", aDesc, bDesc, pDescription)
}

func (m *merger) threeWayMapMerge(ctx context.Context, a, b, parent types.Map, path types.Path) (merged types.Value, err error) {
	apply := func(target candidate, change types.ValueChanged, newVal types.Value) (candidate, error) {
		defer updateProgress(m.progress)
		switch change.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			res, err := target.getValue().(types.Map).Edit().Set(change.Key, newVal).Map(ctx)

			if err != nil {
				return nil, err
			}

			return mapCandidate{res}, nil
		case types.DiffChangeRemoved:
			res, err := target.getValue().(types.Map).Edit().Remove(change.Key).Map(ctx)

			if err != nil {
				return nil, err
			}

			return mapCandidate{res}, nil
		default:
			panic("Not Reached")
		}
	}
	return m.threeWayOrderedSequenceMerge(ctx, mapCandidate{a}, mapCandidate{b}, mapCandidate{parent}, apply, path)
}

func (m *merger) threeWaySetMerge(ctx context.Context, a, b, parent types.Set, path types.Path) (merged types.Value, err error) {
	apply := func(target candidate, change types.ValueChanged, newVal types.Value) (candidate, error) {
		defer updateProgress(m.progress)
		switch change.ChangeType {
		case types.DiffChangeAdded, types.DiffChangeModified:
			se, err := target.getValue().(types.Set).Edit().Insert(ctx, newVal)

			if err != nil {
				return nil, err
			}

			s, err := se.Set(ctx)

			if err != nil {
				return nil, err
			}

			return setCandidate{s}, nil
		case types.DiffChangeRemoved:
			se, err := target.getValue().(types.Set).Edit().Remove(ctx, newVal)

			if err != nil {
				return nil, err
			}

			s, err := se.Set(ctx)

			if err != nil {
				return nil, err
			}

			return setCandidate{s}, nil
		default:
			panic("Not Reached")
		}
	}
	return m.threeWayOrderedSequenceMerge(ctx, setCandidate{a}, setCandidate{b}, setCandidate{parent}, apply, path)
}

func (m *merger) threeWayStructMerge(ctx context.Context, a, b, parent types.Struct, path types.Path) (merged types.Value, err error) {
	apply := func(target candidate, change types.ValueChanged, newVal types.Value) (candidate, error) {
		defer updateProgress(m.progress)
		// Right now, this always iterates over all fields to create a new Struct, because there's no API for adding/removing a field from an existing struct type.
		targetVal := target.getValue().(types.Struct)
		if f, ok := change.Key.(types.String); ok {
			field := string(f)
			data := types.StructData{}
			_ = targetVal.IterFields(func(name string, v types.Value) error {
				if name != field {
					data[name] = v
				}

				return nil
			})
			if change.ChangeType == types.DiffChangeAdded || change.ChangeType == types.DiffChangeModified {
				data[field] = newVal
			}

			st, err := types.NewStruct(m.vrw.Format(), targetVal.Name(), data)

			if err != nil {
				return nil, err
			}

			return structCandidate{st}, nil
		}

		return nil, errors.New("bad key type in diff")
	}
	return m.threeWayOrderedSequenceMerge(ctx, structCandidate{a}, structCandidate{b}, structCandidate{parent}, apply, path)
}

func listAssert(ctx context.Context, vrw types.ValueReadWriter, a, b, parent types.Value) (aList, bList, pList types.List, ok bool, err error) {
	var aOk, bOk, pOk bool
	aList, aOk = a.(types.List)
	bList, bOk = b.(types.List)
	if parent != nil {
		pList, pOk = parent.(types.List)
	} else {
		pList, err = types.NewList(ctx, vrw)

		if err != nil {
			return types.EmptyList, types.EmptyList, types.EmptyList, false, err
		}

		pOk = true
	}

	return aList, bList, pList, aOk && bOk && pOk, nil
}

func mapAssert(ctx context.Context, vrw types.ValueReadWriter, a, b, parent types.Value) (aMap, bMap, pMap types.Map, ok bool, err error) {
	var aOk, bOk, pOk bool
	aMap, aOk = a.(types.Map)
	bMap, bOk = b.(types.Map)
	if parent != nil {
		pMap, pOk = parent.(types.Map)
	} else {
		pMap, err = types.NewMap(ctx, vrw)

		if err != nil {
			return types.EmptyMap, types.EmptyMap, types.EmptyMap, false, err
		}

		pOk = true
	}

	return aMap, bMap, pMap, aOk && bOk && pOk, nil
}

func refAssert(ctx context.Context, a, b, parent types.Value, vrw types.ValueReadWriter) (aValue, bValue, pValue types.Value, ok bool, err error) {
	var aOk, bOk, pOk bool
	var aRef, bRef, pRef types.Ref
	aRef, aOk = a.(types.Ref)
	bRef, bOk = b.(types.Ref)
	if !aOk || !bOk {
		return
	}

	aValue, err = aRef.TargetValue(ctx, vrw)

	if err != nil {
		return nil, nil, nil, false, err
	}

	bValue, err = bRef.TargetValue(ctx, vrw)

	if err != nil {
		return nil, nil, nil, false, err
	}

	if parent != nil {
		if pRef, pOk = parent.(types.Ref); pOk {
			pValue, err = pRef.TargetValue(ctx, vrw)

			if err != nil {
				return nil, nil, nil, false, err
			}

		}
	} else {
		pOk = true // parent == nil is still OK. It just leaves pValue as nil.
	}
	return aValue, bValue, pValue, aOk && bOk && pOk, nil
}

func setAssert(ctx context.Context, vrw types.ValueReadWriter, a, b, parent types.Value) (aSet, bSet, pSet types.Set, ok bool, err error) {
	var aOk, bOk, pOk bool
	aSet, aOk = a.(types.Set)
	bSet, bOk = b.(types.Set)
	if parent != nil {
		pSet, pOk = parent.(types.Set)
	} else {
		pSet, err = types.NewSet(ctx, vrw)

		if err != nil {
			return types.EmptySet, types.EmptySet, types.EmptySet, false, err
		}

		pOk = true
	}

	return aSet, bSet, pSet, aOk && bOk && pOk, nil
}

func structAssert(a, b, parent types.Value) (aStruct, bStruct, pStruct types.Struct, ok bool, err error) {
	var aOk, bOk, pOk bool
	aStruct, aOk = a.(types.Struct)
	bStruct, bOk = b.(types.Struct)
	if aOk && bOk {
		if aStruct.Name() == bStruct.Name() {
			if parent != nil {
				pStruct, pOk = parent.(types.Struct)
			} else {
				pStruct, err = types.NewStruct(aStruct.Format(), aStruct.Name(), nil)

				if err != nil {
					es := types.EmptyStruct(aStruct.Format())
					return es, es, es, false, err
				}

				pOk = true
			}

			return aStruct, bStruct, pStruct, pOk, err
		}
	}

	return
}
