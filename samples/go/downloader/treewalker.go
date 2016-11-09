// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

// TreeWalkCallback defines a function prototype submitting successive values
// from TreeWalk to the caller. At any point, the function can return true
// to prevent traversing any of the current value's children.
type TreeWalkCallback func(p types.Path, parent, v types.Value) (stop bool)

// TreeWalk walks over a noms graph starting at 'v' and calls 'twcb' for each
// value it encounters. It calls 'twcb' with a path from the original value to
// the current value, the current value's parent, and the current value. TreeWalk
// also takes a ValueReader so that it can traverse values across refs.
func TreeWalk(vr types.ValueReader, p types.Path, v types.Value, twcb TreeWalkCallback) {
	var processVal func(p types.Path, parent, v types.Value)
	var processRef func(p types.Path, parent types.Value, r types.Ref)
	visited := map[hash.Hash]bool{}

	processVal = func(p types.Path, parent, v types.Value) {
		if sr, ok := v.(types.Ref); ok {
			processRef(p, parent, sr)
			return
		}

		if !twcb(p, parent, v) {
			switch value := v.(type) {
			case types.List:
				value.IterAll(func(c types.Value, index uint64) {
					p1 := p.Append(types.NewIndexPath(types.Number(index)))
					processVal(p1, v, c)
				})
			case types.Set:
				value.IterAll(func(c types.Value) {
					p1 := p.Append(types.NewHashIndexPath(c.Hash()))
					processVal(p1, v, c)
				})
			case types.Map:
				value.IterAll(func(k, c types.Value) {
					var kp1, vp1 types.Path
					if types.ValueCanBePathIndex(k) {
						kp1 = p.Append(types.NewIndexIntoKeyPath(k))
						vp1 = p.Append(types.NewIndexPath(k))
					} else {
						kp1 = p.Append(types.NewHashIndexIntoKeyPath(k.Hash()))
						vp1 = p.Append(types.NewHashIndexPath(k.Hash()))
					}
					processVal(kp1, v, k)
					processVal(vp1, v, c)
				})
			case types.Struct:
				value.Type().Desc.(types.StructDesc).IterFields(func(name string, typ *types.Type) {
					p1 := p.Append(types.NewFieldPath(name))
					c := value.Get(name)
					processVal(p1, v, c)
				})
			}
		}
	}

	// Todo: this resolves the path through the ref transparently. Is that right?
	processRef = func(p types.Path, parent types.Value, r types.Ref) {
		if visited[r.TargetHash()] {
			return
		}
		visited[r.TargetHash()] = true

		target := r.TargetHash()
		c := vr.ReadValue(target)
		d.PanicIfTrue(c == nil)
		processVal(p, parent, c)
	}

	processVal(p, nil, v)
}
