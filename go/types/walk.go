// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/hash"
)

type SkipValueCallback func(v Value) bool

// WalkValues loads prolly trees progressively by walking down the tree. We don't wants to invoke
// the value callback on internal sub-trees (which are valid values) because they are not logical
// values in the graph
type valueRec struct {
	v  Value
	cb bool
}

const maxRefCount = 1 << 12 // ~16MB of data

// WalkValues recursively walks over all types. Values reachable from r and calls cb on them.
func WalkValues(target Value, vr ValueReader, cb SkipValueCallback) {
	visited := hash.HashSet{}
	refs := map[hash.Hash]bool{}
	values := []valueRec{{target, true}}

	for len(values) > 0 || len(refs) > 0 {
		for len(values) > 0 {
			rec := values[len(values)-1]
			values = values[:len(values)-1]

			v := rec.v
			if rec.cb && cb(v) {
				continue
			}

			if _, ok := v.(Blob); ok {
				continue // don't traverse into blob ptrees
			}

			if r, ok := v.(Ref); ok {
				refs[r.TargetHash()] = true
				continue
			}

			if col, ok := v.(Collection); ok && !col.sequence().isLeaf() {
				ms := col.sequence().(metaSequence)
				for _, mt := range ms.tuples {
					if mt.child != nil {
						values = append(values, valueRec{mt.child, false})
					} else {
						refs[mt.ref.TargetHash()] = false
					}
				}
				continue
			}

			v.WalkValues(func(sv Value) {
				values = append(values, valueRec{sv, true})
			})
		}

		if len(refs) == 0 {
			continue
		}

		hs := hash.HashSet{}
		oldRefs := refs
		refs = map[hash.Hash]bool{}
		for h := range oldRefs {
			if _, ok := visited[h]; ok {
				continue
			}

			if len(hs) >= maxRefCount {
				refs[h] = oldRefs[h]
				continue
			}

			hs.Insert(h)
			visited.Insert(h)
		}

		if len(hs) > 0 {
			valueChan := make(chan Value, len(hs))
			vr.ReadManyValues(hs, valueChan)
			close(valueChan)
			for sv := range valueChan {
				values = append(values, valueRec{sv, oldRefs[sv.Hash()]})
			}
		}
	}
}

func mightContainStructs(t *Type) (mightHaveStructs bool) {
	if t.TargetKind() == StructKind || t.TargetKind() == ValueKind {
		mightHaveStructs = true
		return
	}

	t.WalkValues(func(v Value) {
		mightHaveStructs = mightHaveStructs || mightContainStructs(v.(*Type))
	})

	return
}

// WalkDifferentStructs Efficiently returns the set of |removed| (reachable from |last|, but not
// |current|) and |added| (reachable from |current| but not |last|) types.Structs. Internally
// avoids visiting chunks which are common to both value graphs.
func WalkDifferentStructs(last, current Value, vr ValueReader) (added, removed map[hash.Hash]Struct) {
	added, removed = map[hash.Hash]Struct{}, map[hash.Hash]Struct{}

	oldRefs, currentRefs := RefByHeight{}, RefByHeight{}
	oldValues, currentValues := ValueSlice{}, ValueSlice{}
	if last != nil {
		if current != nil && last.Equals(current) {
			return // same values
		}

		oldValues = append(oldValues, last)
	}
	if current != nil {
		currentValues = append(currentValues, current)
	}

	walkLocalValues := func(values *ValueSlice, structs map[hash.Hash]Struct, refs *RefByHeight) {
		for len(*values) > 0 {
			v := (*values)[len(*values)-1]
			*values = (*values)[:len(*values)-1]

			if !mightContainStructs(TypeOf(v)) {
				continue
			}

			if _, ok := v.(Blob); ok {
				continue // don't traverse into blob ptrees
			}

			if r, ok := v.(Ref); ok {
				refs.PushBack(r)
				continue
			}

			if col, ok := v.(Collection); ok && !col.sequence().isLeaf() {
				ms := col.sequence().(metaSequence)
				for _, mt := range ms.tuples {
					if mt.child != nil {
						*values = append(*values, mt.child)
					} else if mightContainStructs(TypeOf(mt.ref)) {
						refs.PushBack(mt.ref)
					}
				}
				continue
			}

			if s, ok := v.(Struct); ok {
				structs[s.Hash()] = s
			}

			v.WalkValues(func(sv Value) {
				*values = append(*values, sv)
			})
		}

		sort.Sort(refs)
	}

	for len(oldValues) > 0 || len(currentValues) > 0 || oldRefs.Len() > 0 || currentRefs.Len() > 0 {
		// Iterate over all "loaded values". Record structs and refs of unloaded values.
		walkLocalValues(&oldValues, removed, &oldRefs)
		walkLocalValues(&currentValues, added, &currentRefs)

		if len(oldRefs) == 0 && len(currentRefs) == 0 {
			continue
		}

		oldRefsToLoad := hash.HashSet{}
		currentRefsToLoad := hash.HashSet{}

		oldMaxHeight := uint64(0)
		if oldRefs.Len() > 0 {
			oldMaxHeight = oldRefs.MaxHeight()
		}
		currentMaxHeight := uint64(0)
		if currentRefs.Len() > 0 {
			currentMaxHeight = currentRefs.MaxHeight()
		}

		for oldRefs.Len() > 0 && oldRefs.MaxHeight() > currentMaxHeight {
			// Load all taller old refs
			oldRefsToLoad.Insert(oldRefs.PopBack().TargetHash())
		}

		for len(oldRefsToLoad) == 0 && currentRefs.Len() > 0 && currentRefs.MaxHeight() > oldMaxHeight {
			// Load all taller new refs
			currentRefsToLoad.Insert(currentRefs.PopBack().TargetHash())
		}

		if len(oldRefsToLoad) == 0 && len(currentRefsToLoad) == 0 && currentMaxHeight > 0 {
			commonHeight := currentMaxHeight

			for oldRefs.Len() > 0 && oldRefs.MaxHeight() >= commonHeight {
				// Load all taller old refs
				oldRefsToLoad.Insert(oldRefs.PopBack().TargetHash())
			}

			for currentRefs.Len() > 0 && currentRefs.MaxHeight() >= commonHeight {
				h := currentRefs.PopBack().TargetHash()
				if _, ok := oldRefsToLoad[h]; ok {
					delete(oldRefsToLoad, h)
				} else {
					currentRefsToLoad.Insert(h)
				}
			}
		}

		// oldRefsToLoad and currentRefsToLoad are fully disjoint. Insert all old into current and use
		// oldRefs to later differentiate
		for h := range oldRefsToLoad {
			currentRefsToLoad.Insert(h)
		}

		valueChan := make(chan Value, len(currentRefsToLoad))
		vr.ReadManyValues(currentRefsToLoad, valueChan)
		close(valueChan)
		for v := range valueChan {
			if _, ok := oldRefsToLoad[v.Hash()]; ok {
				oldValues = append(oldValues, v)
			} else {
				currentValues = append(currentValues, v)
			}
		}
	}

	// Remove common
	for h := range added {
		if _, ok := removed[h]; ok {
			delete(added, h)
			delete(removed, h)
		}
	}

	return
}
