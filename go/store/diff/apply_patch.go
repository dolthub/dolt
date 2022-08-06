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

package diff

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
)

// Apply applies a Patch (list of diffs) to a graph. It fulfills the
// following contract:
//
//	Given 2 Noms graphs: a1 and a2:
//	  ApplyPatch(a1, Diff(a1, a2)) == a2
//
// This is useful for IncrementalUpdate() and possibly other problems. See
// updater.go for more information.
//
// This function uses a patchStack to maintain state of the graph as it cycles
// through the diffs in a patch, applying them to 'root' one by one. Because the
// Difference objects in the patch can be sorted according to their path, each
// one is applied in order. When done in combination with the stack, this enables
// all Differences that change a particular node to be applied to that node
// before it gets assigned back to it's parent.
func Apply(ctx context.Context, nbf *types.NomsBinFormat, root types.Value, patch Patch) (types.Value, error) {
	if len(patch) == 0 {
		return root, nil
	}

	var lastPath types.Path
	stack := patchStack{}
	types.SortWithErroringLess(PatchSort{patch, nbf})

	// Push the element on the stack that corresponds to the root
	// node.
	stack.push(nil, nil, types.DiffChangeModified, root, nil, nil)

	for _, dif := range patch {
		// get the path where this dif needs to be applied
		p := dif.Path

		// idx will hold the index of the last common element between p and
		// lastPath (p from the last iteration).
		var idx int

		// p can be identical to lastPath in certain cases. For example, when
		// one item gets removed from a list at the same place another item
		// is added to it. In this case, we need pop the last operation of the
		// stack early and set the idx to be the len(p) - 1.
		// Otherwise, if the paths are different we can call commonPrefixCount()
		if len(p) > 0 && p.Equals(lastPath) {
			_, err := stack.pop(ctx)

			if err != nil {
				return nil, err
			}

			idx = len(p) - 1
		} else {
			idx = commonPrefixCount(lastPath, p)
		}
		lastPath = p

		// if the stack has elements on it leftover from the last iteration. Pop
		// those elements until the stack only has values in it that are
		// referenced by this p. Popping an element on the stack, folds that
		// value into it's parent.
		for idx < stack.Len()-1 {
			_, err := stack.pop(ctx)

			if err != nil {
				return nil, err
			}
		}

		// tail is the part of the current path that has not yet been pushed
		// onto the stack. Iterate over those pathParts and push those values
		// onto the stack.
		tail := p[idx:]
		for i, pp := range tail {
			top := stack.top()
			parent := top.newestValue()
			oldValue, err := pp.Resolve(ctx, parent, nil)

			if err != nil {
				return nil, err
			}

			var newValue types.Value
			if i == len(tail)-1 { // last pathPart in this path
				newValue = oldValue
				oldValue = dif.OldValue
			}
			// Any intermediate elements on the stack will have a changeType
			// of modified.  Leaf elements will be updated below to reflect the
			// actual changeType.
			stack.push(p, pp, types.DiffChangeModified, oldValue, newValue, dif.NewKeyValue)
		}

		// Update the top element in the stack with changeType from the dif and
		// the NewValue from the diff
		se := stack.top()
		se.newValue = dif.NewValue
		se.changeType = dif.ChangeType
	}

	// We're done applying diffs to the graph. Pop any elements left on the
	// stack and return the new root.
	var newRoot stackElem
	for stack.Len() > 0 {
		var err error
		newRoot, err = stack.pop(ctx)

		if err != nil {
			return nil, err
		}
	}
	return newRoot.newValue, nil
}

// updateNode handles the actual update of a node. It uses 'pp' to get the
// information that it needs to update 'parent' with 'newVal'. 'oldVal' is also
// passed in so that Sets can be updated correctly. This function is used by
// the patchStack Pop() function to merge values into a new graph.
func (stack *patchStack) updateNode(ctx context.Context, top *stackElem, parent types.Value) (types.Value, error) {
	d.PanicIfTrue(parent == nil)
	switch part := top.pathPart.(type) {
	case types.FieldPath:
		switch top.changeType {
		case types.DiffChangeAdded:
			return parent.(types.Struct).Set(part.Name, top.newValue)
		case types.DiffChangeRemoved:
			return parent.(types.Struct).Delete(part.Name)
		case types.DiffChangeModified:
			return parent.(types.Struct).Set(part.Name, top.newValue)
		}
	case types.IndexPath:
		switch el := parent.(type) {
		case types.List:
			idx := uint64(part.Index.(types.Float))
			offset := stack.adjustIndexOffset(top.path, top.changeType)
			realIdx := idx + uint64(offset)
			var nv types.Value
			switch top.changeType {
			case types.DiffChangeAdded:
				if realIdx > el.Len() {
					return el.Edit().Append(top.newValue).List(ctx)
				} else {
					return el.Edit().Insert(realIdx, top.newValue).List(ctx)
				}

			case types.DiffChangeRemoved:
				return el.Edit().RemoveAt(realIdx).List(ctx)
			case types.DiffChangeModified:
				return el.Edit().Set(realIdx, top.newValue).List(ctx)
			}
			return nv, nil
		case types.Map:
			switch top.changeType {
			case types.DiffChangeAdded:
				return el.Edit().Set(part.Index, top.newValue).Map(ctx)
			case types.DiffChangeRemoved:
				return el.Edit().Remove(part.Index).Map(ctx)
			case types.DiffChangeModified:
				if part.IntoKey {
					newPart := types.IndexPath{Index: part.Index}
					ov, err := newPart.Resolve(ctx, parent, nil)

					if err != nil {
						return nil, err
					}

					return el.Edit().Remove(part.Index).Set(top.newValue, ov).Map(ctx)
				}
				return el.Edit().Set(part.Index, top.newValue).Map(ctx)
			}
		case types.Set:
			if top.oldValue != nil {
				se, err := el.Edit().Remove(top.oldValue)

				if err != nil {
					return nil, err
				}

				el, err = se.Set(ctx)

				if err != nil {
					return nil, err
				}
			}

			if top.newValue != nil {
				se, err := el.Edit().Insert(top.newValue)

				if err != nil {
					return nil, err
				}

				el, err = se.Set(ctx)

				if err != nil {
					return nil, err
				}
			}

			return el, nil
		}
	case types.HashIndexPath:
		switch el := parent.(type) {
		case types.Set:
			switch top.changeType {
			case types.DiffChangeAdded:
				se, err := el.Edit().Insert(top.newValue)

				if err != nil {
					return nil, err
				}

				return se.Set(ctx)
			case types.DiffChangeRemoved:
				se, err := el.Edit().Remove(top.oldValue)

				if err != nil {
					return nil, err
				}

				return se.Set(ctx)
			case types.DiffChangeModified:
				se, err := el.Edit().Remove(top.oldValue)

				if err != nil {
					return nil, err
				}

				se, err = se.Insert(top.newValue)

				if err != nil {
					return nil, err
				}

				return se.Set(ctx)
			}
		case types.Map:
			keyPart := types.HashIndexPath{Hash: part.Hash, IntoKey: true}
			k, err := keyPart.Resolve(ctx, parent, nil)

			if err != nil {
				return nil, err
			}

			switch top.changeType {
			case types.DiffChangeAdded:
				k := top.newKeyValue
				return el.Edit().Set(k, top.newValue).Map(ctx)
			case types.DiffChangeRemoved:
				return el.Edit().Remove(k).Map(ctx)
			case types.DiffChangeModified:
				if part.IntoKey {
					v, found, err := el.MaybeGet(ctx, k)

					if err != nil {
						return nil, err
					}

					d.PanicIfFalse(found)
					return el.Edit().Remove(k).Set(top.newValue, v).Map(ctx)
				}
				return el.Edit().Set(k, top.newValue).Map(ctx)
			}
		}
	}
	panic(fmt.Sprintf("unreachable, pp.(type): %T", top.pathPart))
}

// Returns the count of the number of PathParts that two paths have in a common
// prefix. The paths '.field1' and '.field2' have a 0 length common prefix.
// Todo: move to types.Path?
func commonPrefixCount(p1, p2 types.Path) int {
	cnt := 0

	for i, pp1 := range p1 {
		var pp2 types.PathPart
		if i < len(p2) {
			pp2 = p2[i]
		}
		if pp1 != pp2 {
			return cnt
		}
		cnt += 1
	}
	return cnt
}

type stackElem struct {
	path        types.Path
	pathPart    types.PathPart // from parent Value to this Value
	changeType  types.DiffChangeType
	oldValue    types.Value // can be nil if newValue is not nil
	newValue    types.Value // can be nil if oldValue is not nil
	newKeyValue types.Value
}

// newestValue returns newValue if not nil, otherwise oldValue. This is useful
// when merging. Elements on the stack were 'push'ed there with the oldValue.
// newValue may have been set when a value was 'pop'ed above it. This method
// returns the last value that has been set.
func (se stackElem) newestValue() types.Value {
	if se.newValue != nil {
		return se.newValue
	}
	return se.oldValue
}

type patchStack struct {
	vals     []stackElem
	lastPath types.Path
	addCnt   int
	rmCnt    int
}

func (stack *patchStack) push(p types.Path, pp types.PathPart, changeType types.DiffChangeType, oldValue, newValue, newKeyValue types.Value) {
	stack.vals = append(stack.vals, stackElem{path: p, pathPart: pp, changeType: changeType, oldValue: oldValue, newValue: newValue, newKeyValue: newKeyValue})
}

func (stack *patchStack) top() *stackElem {
	return &stack.vals[len(stack.vals)-1]
}

// pop applies the change to the graph. When an element is 'pop'ed from the stack,
// this function uses the pathPart to merge that value into it's parent.
func (stack *patchStack) pop(ctx context.Context) (stackElem, error) {
	top := stack.top()
	stack.vals = stack.vals[:len(stack.vals)-1]
	if stack.Len() > 0 {
		newTop := stack.top()
		parent := newTop.newestValue()

		var err error
		newTop.newValue, err = stack.updateNode(ctx, top, parent)

		if err != nil {
			return stackElem{}, err
		}
	}
	return *top, nil
}

func (stack *patchStack) Len() int {
	return len(stack.vals)
}

// adjustIndexOffset returns an offset that needs to be added to list indexes
// when applying diffs to lists. Diffs are applied to lists beginning at the 0th
// element. Changes to the list mean that subsequent changes to the same list
// have to be adjusted accordingly. The stack keeps state for each list as it's
// processed so updateNode() can get the correct index.
// Whenever a list is encountered, diffs consist of add & remove operations. The
// offset is calculated by keeping a count of each add & remove. Due to the way
// way diffs are calculated, no offset is ever needed for 'add' operations. The
// offset for 'remove' operations are calculated as:
//
//	stack.addCnt - stack.rmCnt
func (stack *patchStack) adjustIndexOffset(p types.Path, changeType types.DiffChangeType) (res int) {
	parentPath := p[:len(p)-1]

	// parentPath is different than the last parentPath so reset counters
	if stack.lastPath == nil || !stack.lastPath.Equals(parentPath) {
		stack.lastPath = parentPath
		stack.addCnt = 0
		stack.rmCnt = 0
	}

	// offset for 'Add' operations are always 0, 'Remove' ops offset are
	// calculated here
	if changeType == types.DiffChangeRemoved {
		res = stack.addCnt - stack.rmCnt
	}

	// Bump up the appropriate cnt for this operation.
	switch changeType {
	case types.DiffChangeAdded:
		stack.addCnt += 1
	case types.DiffChangeRemoved:
		stack.rmCnt += 1
	}
	return
}
