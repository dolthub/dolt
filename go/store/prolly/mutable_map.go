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

	"github.com/dolthub/dolt/go/store/skip"

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
		// todo(andy): put again?
		// synchronously flush overlay
		mut.m, err = mut.Map(ctx)
		mut.overlay = newMemoryMap(mut.m.keyDesc)
	}
	return
}

func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	value, ok := mut.overlay.list.Get(key)
	if ok {
		if value == nil {
			// there is a pending delete of |key| in |mut.overlay|.
			key = nil
		}
		return cb(key, value)
	}

	return mut.m.Get(ctx, key, cb)
}

func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	err = mut.Get(ctx, key, func(key, value val.Tuple) (err error) {
		ok = key != nil
		return
	})
	return
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
	var iter *skip.ListIter
	if rng.Start.Unbound {
		if rng.Reverse {
			iter = mut.overlay.list.IterAtEnd()
		} else {
			iter = mut.overlay.list.IterAtStart()
		}
	} else {
		iter = mut.overlay.list.IterAt(rng.Start.Key)
	}

	var err error
	var cur *nodeCursor
	if rng.Start.Unbound {
		if rng.Reverse {
			cur, err = mut.m.cursorAtEnd(ctx)
		} else {
			cur, err = mut.m.cursorAtStart(ctx)
		}
	} else {
		cur, err = mut.m.cursorAtkey(ctx, rng.Start.Key)
	}
	if err != nil {
		return MapRangeIter{}, err
	}

	mri := MapRangeIter{
		memCur: memTupleCursor{iter: iter},
		proCur: mapTupleCursor{cur: cur},
		rng:    rng,
	}

	if err = startInRange(ctx, mri); err != nil {
		return MapRangeIter{}, err
	}

	return mri, nil
}
