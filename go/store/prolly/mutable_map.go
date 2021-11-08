// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/store/val"
)

type MutableMap struct {
	m       Map
	overlay memoryMap
}

func newMutableMap(m Map) MutableMap {
	return MutableMap{
		m:       m,
		overlay: newMemoryMap(m.keyDesc),
	}
}

func (mut MutableMap) Map(ctx context.Context) (Map, error) {
	return applyEdits(ctx, mut.m, mut.overlay.mutations())
}

func (mut MutableMap) Count() uint64 {
	return mut.m.Count() + mut.overlay.Count()
}

func (mut MutableMap) Put(ctx context.Context, key, value val.Tuple) (err error) {
	ok := mut.overlay.Put(key, value)
	if !ok {
		// synchronously flush overlay
		mut.m, err = mut.Map(ctx)
		mut.overlay = newMemoryMap(mut.m.keyDesc)
	}
	return
}

func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	panic("unimplemented")
}

func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	panic("unimplemented")
}

func (mut MutableMap) IterAll(ctx context.Context) (MapIter, error) {
	panic("unimplemented")
}

func (mut MutableMap) IterValueRange(ctx context.Context, rng ValueRange) (MapIter, error) {
	panic("unimplemented")
}

func (mut MutableMap) IterIndexRange(ctx context.Context, rng IndexRange) (MapIter, error) {
	panic("unimplemented")
}
