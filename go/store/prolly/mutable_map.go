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
	return materializeMutations(ctx, mut.m, mut.overlay.mutations())
}

func (mut MutableMap) Count() uint64 {
	panic("harder than you think!")
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
	var value val.Tuple
	_ = mut.overlay.Get(ctx, key, func(k, v val.Tuple) error {
		if v != nil {
			value = v
		}
		return nil
	})

	if value != nil {
		// |key| found in memCur
		return cb(key, value)
	}

	return mut.m.Get(ctx, key, cb)
}

func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	ok, _ = mut.overlay.Has(ctx, key)
	if ok {
		return
	}
	return mut.m.Has(ctx, key)
}

func (mut MutableMap) IterAll(ctx context.Context) (MapRangeIter, error) {
	rng := Range{
		Start:   RangeCut{Unbound: true},
		Stop:    RangeCut{Unbound: true},
		KeyDesc: mut.m.keyDesc,
		Reverse: false,
	}
	return mut.IterValueRange(ctx, rng)
}

func (mut MutableMap) IterValueRange(ctx context.Context, rng Range) (MapRangeIter, error) {
	mem, err := mut.overlay.IterValueRange(ctx, rng)
	if err != nil {
		return MapRangeIter{}, err
	}

	pro, err := mut.m.IterValueRange(ctx, rng)
	if err != nil {
		return MapRangeIter{}, err
	}

	rng.Reverse = false

	return MapRangeIter{
		memCur: mem.memCur,
		proCur: pro.proCur,
		rng:    rng,
	}, nil
}
