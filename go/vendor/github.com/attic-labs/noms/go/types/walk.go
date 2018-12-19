// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/hash"

type SkipValueCallback func(v Value) bool

// WalkValues loads prolly trees progressively by walking down the tree. We don't wants to invoke
// the value callback on internal sub-trees (which are valid values) because they are not logical
// values in the graph
type valueRec struct {
	v  Value
	cb bool
}

const maxRefCount = 1 << 12 // ~16MB of data

// WalkValues recursively walks over all types.Values reachable from r and calls cb on them.
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

			if col, ok := v.(Collection); ok && !col.asSequence().isLeaf() {
				col.WalkRefs(func(r Ref) {
					refs[r.TargetHash()] = false
				})
				continue
			}

			v.WalkValues(func(sv Value) {
				values = append(values, valueRec{sv, true})
			})
		}

		if len(refs) == 0 {
			continue
		}

		hs := make(hash.HashSlice, 0, len(refs))
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

			hs = append(hs, h)
			visited.Insert(h)
		}

		if len(hs) > 0 {
			readValues := vr.ReadManyValues(hs)
			for i, sv := range readValues {
				values = append(values, valueRec{sv, oldRefs[hs[i]]})
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
