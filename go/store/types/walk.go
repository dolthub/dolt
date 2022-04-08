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

package types

import (
	"context"

	"github.com/dolthub/dolt/go/store/hash"
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

// WalkValues recursively walks over all types.Values reachable from r and calls cb on them.
func WalkValues(ctx context.Context, nbf *NomsBinFormat, target Value, vr ValueReader, cb SkipValueCallback) error {
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
				err := col.walkRefs(nbf, func(r Ref) error {
					refs[r.TargetHash()] = false
					return nil
				})

				if err != nil {
					return err
				}

				continue
			}

			if sm, ok := v.(SerialMessage); ok {
				err := sm.walkRefs(nbf, func(r Ref) error {
					refs[r.TargetHash()] = false
					return nil
				})
				if err != nil {
					return err
				}
				continue
			}

			err := v.WalkValues(ctx, func(sv Value) error {
				values = append(values, valueRec{sv, true})

				return nil
			})

			if err != nil {
				return err
			}
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
			readValues, err := vr.ReadManyValues(ctx, hs)

			if err != nil {
				return err
			}

			for i, sv := range readValues {
				values = append(values, valueRec{sv, oldRefs[hs[i]]})
			}
		}
	}

	return nil
}
