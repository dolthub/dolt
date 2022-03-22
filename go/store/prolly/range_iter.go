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
	"io"

	"github.com/dolthub/dolt/go/store/val"
)

type MapRangeIter interface {
	Next(ctx context.Context) (key, value val.Tuple, err error)
}

var _ MapRangeIter = &prollyRangeIter{}
var _ MapRangeIter = &MutableMapRangeIter{}

type rangeIter interface {
	iterate(ctx context.Context) error
	current() (key, value val.Tuple)
}

var _ rangeIter = emptyIter{}
var _ rangeIter = &prollyRangeIter{}

type emptyIter struct{}

func (e emptyIter) iterate(ctx context.Context) (err error) {
	return
}

func (e emptyIter) current() (key, value val.Tuple) {
	return
}

func NewMutableMapRangeIter(memory, prolly rangeIter, rng Range) MapRangeIter {
	if memory == nil {
		memory = emptyIter{}
	}
	if prolly == nil {
		prolly = emptyIter{}
	}

	return MutableMapRangeIter{
		memory: memory,
		prolly: prolly,
		rng:    rng,
	}
}

// MutableMapRangeIter iterates over a Range of Tuples.
type MutableMapRangeIter struct {
	memory rangeIter
	prolly rangeIter
	rng    Range
}

// Next returns the next pair of Tuples in the Range, or io.EOF if the iter is done.
func (it MutableMapRangeIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	for {
		mk, mv := it.memory.current()
		pk, pv := it.prolly.current()

		if mk == nil && pk == nil {
			// range is exhausted
			return nil, nil, io.EOF
		}

		cmp := it.compareKeys(pk, mk)
		switch {
		case cmp < 0:
			key, value = pk, pv
			if err = it.prolly.iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp > 0:
			key, value = mk, mv
			if err = it.memory.iterate(ctx); err != nil {
				return nil, nil, err
			}

		case cmp == 0:
			// |it.memory| wins ties
			key, value = mk, mv
			if err = it.memory.iterate(ctx); err != nil {
				return nil, nil, err
			}
			if err = it.prolly.iterate(ctx); err != nil {
				return nil, nil, err
			}
		}

		if key != nil && value == nil {
			continue // pending delete
		}

		return key, value, nil
	}
}

func (it MutableMapRangeIter) currentKeys() (memKey, proKey val.Tuple) {
	if it.memory != nil {
		memKey, _ = it.memory.current()
	}
	if it.prolly != nil {
		proKey, _ = it.prolly.current()
	}
	return
}

func (it MutableMapRangeIter) compareKeys(memKey, proKey val.Tuple) int {
	if memKey == nil {
		return 1
	}
	if proKey == nil {
		return -1
	}
	return it.rng.KeyDesc.Compare(memKey, proKey)
}

type prollyRangeIter struct {
	// current tuple location
	curr *nodeCursor
	// non-inclusive range stop
	stop *nodeCursor
}

func (it *prollyRangeIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	if it.curr == nil {
		return nil, nil, io.EOF
	}

	key = it.curr.nd.keys.GetSlice(it.curr.idx)
	value = it.curr.nd.values.GetSlice(it.curr.idx)

	_, err = it.curr.advance(ctx)
	if err != nil {
		return nil, nil, err
	}
	if it.curr.compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return
}

func (it *prollyRangeIter) current() (key, value val.Tuple) {
	// |it.curr| is set to nil when its range is exhausted
	if it.curr != nil && it.curr.valid() {
		key = it.curr.nd.keys.GetSlice(it.curr.idx)
		value = it.curr.nd.values.GetSlice(it.curr.idx)
	}
	return
}

func (it *prollyRangeIter) iterate(ctx context.Context) (err error) {
	_, err = it.curr.advance(ctx)
	if err != nil {
		return err
	}

	if it.curr.compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return
}
