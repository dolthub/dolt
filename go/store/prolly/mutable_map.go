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

const (
	maxPending = 64 * 1024
)

type MutableMap struct {
	prolly  Map
	overlay memoryMap
}

func newMutableMap(m Map) MutableMap {
	return MutableMap{
		prolly:  m,
		overlay: newMemoryMap(m.keyDesc),
	}
}

// Map materializes the pending mutations in the MutableMap.
func (mut MutableMap) Map(ctx context.Context) (Map, error) {
	return materializeMutations(ctx, mut.prolly, mut.overlay.mutations())
}

// Put adds the Tuple pair |key|, |value| to the MutableMap.
func (mut MutableMap) Put(_ context.Context, key, value val.Tuple) error {
	mut.overlay.Put(key, value)
	return nil
}

// Delete deletes the pair keyed by |key| from the MutableMap.
func (mut MutableMap) Delete(_ context.Context, key val.Tuple) error {
	mut.overlay.Delete(key)
	return nil
}

// Get fetches the Tuple pair keyed by |key|, if it exists, and passes it to |cb|.
// If the |key| is not present in the MutableMap, a nil Tuple pair is passed to |cb|.
func (mut MutableMap) Get(ctx context.Context, key val.Tuple, cb KeyValueFn) (err error) {
	value, ok := mut.overlay.list.Get(key)
	if ok {
		if value == nil {
			// there is a pending delete of |key| in |mut.overlay|.
			key = nil
		}
		return cb(key, value)
	}

	return mut.prolly.Get(ctx, key, cb)
}

// Has returns true if |key| is present in the MutableMap.
func (mut MutableMap) Has(ctx context.Context, key val.Tuple) (ok bool, err error) {
	err = mut.Get(ctx, key, func(key, value val.Tuple) (err error) {
		ok = key != nil
		return
	})
	return
}

// IterAll returns a MutableMapRangeIter that iterates over the entire MutableMap.
func (mut MutableMap) IterAll(ctx context.Context) (MapRangeIter, error) {
	rng := Range{
		Start:   RangeCut{Unbound: true},
		Stop:    RangeCut{Unbound: true},
		KeyDesc: mut.prolly.keyDesc,
	}
	return mut.IterRange(ctx, rng)
}

// IterValueRange returns a MutableMapRangeIter that iterates over a Range.
func (mut MutableMap) IterRange(ctx context.Context, rng Range) (MapRangeIter, error) {
	proIter, err := mut.prolly.iterFromRange(ctx, rng)
	if err != nil {
		return MutableMapRangeIter{}, err
	}
	memIter := mut.overlay.iterFromRange(rng)

	return NewMutableMapRangeIter(memIter, proIter, rng), nil
}
