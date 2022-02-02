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

// RangeCut bounds a Range.
type RangeCut struct {
	Key       val.Tuple
	Inclusive bool
	Unbound   bool
}

// Range is a range of Tuples.
type Range struct {
	Start, Stop RangeCut
	KeyDesc     val.TupleDesc
}

func (r Range) insideStart(key val.Tuple) bool {
	if r.Start.Unbound {
		return true
	}

	cmp := r.KeyDesc.Compare(key, r.Start.Key)
	if cmp == 0 {
		return r.Start.Inclusive
	}
	return cmp > 0
}

func (r Range) insideStop(key val.Tuple) bool {
	if r.Stop.Unbound {
		return true
	}

	cmp := r.KeyDesc.Compare(key, r.Stop.Key)
	if cmp == 0 {
		return r.Stop.Inclusive
	}
	return cmp < 0
}

func NewMapRangeIter(memory, prolly rangeIter, rng Range) MapRangeIter {
	if memory == nil {
		memory = emptyIter{}
	}
	if prolly == nil {
		prolly = emptyIter{}
	}

	return MapRangeIter{
		memory: memory,
		prolly: prolly,
		rng:    rng,
	}
}

// MapRangeIter iterates over a Range of Tuples.
type MapRangeIter struct {
	memory rangeIter
	prolly rangeIter
	rng    Range
}

type rangeIter interface {
	iterate(ctx context.Context) error
	current() (key, value val.Tuple)
}

// Next returns the next pair of Tuples in the Range, or io.EOF if the iter is done.
func (it MapRangeIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
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

func (it MapRangeIter) currentKeys() (memKey, proKey val.Tuple) {
	if it.memory != nil {
		memKey, _ = it.memory.current()
	}
	if it.prolly != nil {
		proKey, _ = it.prolly.current()
	}
	return
}

func (it MapRangeIter) compareKeys(memKey, proKey val.Tuple) int {
	if memKey == nil {
		return 1
	}
	if proKey == nil {
		return -1
	}
	return it.rng.KeyDesc.Compare(memKey, proKey)
}

type emptyIter struct{}

var _ rangeIter = emptyIter{}

func (e emptyIter) iterate(ctx context.Context) (err error) {
	return
}

func (e emptyIter) current() (key, value val.Tuple) {
	return
}

// GreaterRange defines a Range of Tuples greater than |lo|.
func GreaterRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Key:       start,
			Inclusive: false,
		},
		Stop: RangeCut{
			Unbound: true,
		},
		KeyDesc: desc,
	}
}

// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |lo|.
func GreaterOrEqualRange(start val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Key:       start,
			Inclusive: true,
		},
		Stop: RangeCut{
			Unbound: true,
		},
		KeyDesc: desc,
	}
}

// LesserRange defines a Range of Tuples less than |last|.
func LesserRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Unbound: true,
		},
		Stop: RangeCut{
			Key:       stop,
			Inclusive: false,
		},
		KeyDesc: desc,
	}
}

// LesserOrEqualRange defines a Range of Tuples less than or equal to |last|.
func LesserOrEqualRange(stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Unbound: true,
		},
		Stop: RangeCut{
			Key:       stop,
			Inclusive: true,
		},
		KeyDesc: desc,
	}
}

// OpenRange defines a non-inclusive Range of Tuples from |lo| to |last|.
func OpenRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Key:       start,
			Inclusive: false,
		},
		Stop: RangeCut{
			Key:       stop,
			Inclusive: false,
		},
		KeyDesc: desc,
	}
}

// OpenStartRange defines a half-open Range of Tuples from |lo| to |last|.
func OpenStartRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Key:       start,
			Inclusive: false,
		},
		Stop: RangeCut{
			Key:       stop,
			Inclusive: true,
		},
		KeyDesc: desc,
	}
}

// OpenStopRange defines a half-open Range of Tuples from |lo| to |last|.
func OpenStopRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Key:       start,
			Inclusive: true,
		},
		Stop: RangeCut{
			Key:       stop,
			Inclusive: false,
		},
		KeyDesc: desc,
	}
}

// ClosedRange defines an inclusive Range of Tuples from |lo| to |last|.
func ClosedRange(start, stop val.Tuple, desc val.TupleDesc) Range {
	return Range{
		Start: RangeCut{
			Key:       start,
			Inclusive: true,
		},
		Stop: RangeCut{
			Key:       stop,
			Inclusive: true,
		},
		KeyDesc: desc,
	}
}
