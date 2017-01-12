// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package walk implements an API for iterating on Noms values.
package walk

import (
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

type SkipValueCallback func(v types.Value) bool

// WalkValues loads prolly trees progressively by walking down the tree. We don't wants to invoke
// the value callback on internal sub-trees (which are valid values) because they are not logical
// values in the graph
type valueRec struct {
	v  types.Value
	cb bool
}

const maxRefCount = 1 << 12 // ~16MB of data

// WalkValues recursively walks over all types. Values reachable from r and calls cb on them.
// TODO: This will only work on graphs of data and are committed to |vr|.
func WalkValues(target types.Value, vr types.ValueReader, cb SkipValueCallback) {
	visited := hash.HashSet{}
	refs := map[hash.Hash]bool{}
	values := []valueRec{{target, true}}

	for len(values) > 0 || len(refs) > 0 {
		for len(values) > 0 {
			rec := values[len(values)-1]
			values = values[:len(values)-1]

			if rec.cb && cb(rec.v) {
				continue
			}

			v := rec.v
			if r, ok := v.(types.Ref); ok {
				refs[r.TargetHash()] = true
				continue
			}

			if col, ok := v.(types.Collection); ok && !col.IsLeaf() {
				col.WalkRefs(func(r types.Ref) {
					refs[r.TargetHash()] = false
				})
				continue
			}

			v.WalkValues(func(sv types.Value) {
				values = append(values, valueRec{sv, true})
			})
		}

		if len(refs) == 0 {
			continue
		}

		hs := hash.HashSet{}
		oldRefs := refs
		refs = map[hash.Hash]bool{}
		for h, _ := range oldRefs {
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
			valueChan := make(chan types.Value, len(hs))
			vr.ReadManyValues(hs, valueChan)
			close(valueChan)
			for sv := range valueChan {
				values = append(values, valueRec{sv, oldRefs[sv.Hash()]})
			}
		}
	}
}
