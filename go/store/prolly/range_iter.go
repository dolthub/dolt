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

	"github.com/dolthub/dolt/go/store/prolly/tree"

	"github.com/dolthub/dolt/go/store/val"
)

type MapRangeIter kvIter[val.Tuple, val.Tuple]

var _ MapRangeIter = emptyIter{}
var _ MapRangeIter = &prollyRangeIter{}
var _ MapRangeIter = &pointLookup{}
var _ MapRangeIter = &MutableMapRangeIter{}

type pointLookup struct {
	k, v val.Tuple
}

func (p *pointLookup) Next(context.Context) (key, value val.Tuple, err error) {
	if p.k == nil || p.v == nil {
		err = io.EOF
	} else {
		key, value = p.k, p.v
		p.k, p.v = nil, nil
	}
	return
}

type rangeIter interface {
	iterate(ctx context.Context) error
	current() (key, value val.Tuple)
}

var _ rangeIter = emptyIter{}
var _ rangeIter = &prollyRangeIter{}

type emptyIter struct{}

func (e emptyIter) Next(context.Context) (val.Tuple, val.Tuple, error) {
	return nil, nil, io.EOF
}

func (e emptyIter) iterate(ctx context.Context) (err error) {
	return
}

func (e emptyIter) current() (key, value val.Tuple) {
	return
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
	return it.rng.Desc.Compare(memKey, proKey)
}

type prollyRangeIter struct {
	// current tuple location
	curr *tree.Cursor
	// non-inclusive range stop
	stop *tree.Cursor
}

func (it *prollyRangeIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	if it.curr == nil {
		return nil, nil, io.EOF
	}

	k, v := tree.CurrentCursorItems(it.curr)
	key, value = val.Tuple(k), val.Tuple(v)

	_, err = it.curr.Advance(ctx)
	if err != nil {
		return nil, nil, err
	}
	if it.curr.Compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return
}

func (it *prollyRangeIter) current() (key, value val.Tuple) {
	// |it.curr| is set to nil when its range is exhausted
	if it.curr != nil && it.curr.Valid() {
		k, v := tree.CurrentCursorItems(it.curr)
		key, value = val.Tuple(k), val.Tuple(v)
	}
	return
}

func (it *prollyRangeIter) iterate(ctx context.Context) (err error) {
	_, err = it.curr.Advance(ctx)
	if err != nil {
		return err
	}

	if it.curr.Compare(it.stop) >= 0 {
		// past the end of the range
		it.curr = nil
	}

	return
}
