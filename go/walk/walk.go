// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package walk implements an API for iterating on Noms values.
package walk

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

type SkipValueCallback func(v types.Value) bool

// WalkValues recursively walks over all types. Values reachable from r and calls cb on them.
func WalkValues(target types.Value, vr types.ValueReader, cb SkipValueCallback) {
	var processRef func(r types.Ref)
	var processVal func(v types.Value)
	visited := map[hash.Hash]bool{}

	processVal = func(v types.Value) {
		if cb(v) {
			return
		}
		if sr, ok := v.(types.Ref); ok {
			processRef(sr)
		} else {
			v.WalkValues(processVal)
		}
	}

	processRef = func(r types.Ref) {
		target := r.TargetHash()
		if visited[target] {
			return
		}
		visited[target] = true
		v := vr.ReadValue(target)
		if v == nil {
			d.Chk.Fail("Attempt to visit absent ref:%s", target.String())
			return
		}

		processVal(v)
	}

	//Process initial value
	processVal(target)

}
