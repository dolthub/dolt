// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/types"
)

func newCompletenessChecker() *completenessChecker {
	return &completenessChecker{hash.HashSet{}}
}

type completenessChecker struct {
	unresolved hash.HashSet
}

// AddRefs adds all the refs in v to the set of refs that PanicIfDangling()
// checks.
func (cc *completenessChecker) AddRefs(v types.Value) {
	cc.unresolved.Remove(v.Hash())
	v.WalkRefs(func(ref types.Ref) {
		cc.unresolved.Insert(ref.TargetHash())
	})
}

// PanicIfDangling panics if any refs in unresolved point to chunks not
// present in cs.
func (cc *completenessChecker) PanicIfDangling(cs chunks.ChunkStore) {
	present := cs.HasMany(cc.unresolved)
	absent := hash.HashSlice{}
	for h := range cc.unresolved {
		if !present.Has(h) {
			absent = append(absent, h)
		}
	}
	if len(absent) != 0 {
		d.Panic("Found dangling references to %v", absent)
	}
}
