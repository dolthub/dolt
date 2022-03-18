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
	"sort"

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

// MergeRanges merges overlapping ranges.
func MergeRanges(ranges ...Range) (merged []Range, err error) {
	if len(ranges) < 2 {
		return ranges, nil
	}

	// validate that ranges share a common prefix
	dd := make([]val.TupleDesc, len(ranges))
	for i := range dd {
		dd[i] = ranges[i].KeyDesc
	}
	if err = val.ValidateCommonPrefix(dd...); err != nil {
		return nil, err
	}

	// todo(andy): are mergeable ranges always
	//  adjacent if we sort by range start?
	SortRanges(ranges)

	var agg = ranges[0]
	for _, rng := range ranges {
		if rangesOverlap(agg, rng) {
			agg = mergeRanges(agg, rng)
		} else {
			merged = append(merged, agg)
			agg = rng
		}
	}
	merged = append(merged, agg)

	return merged, nil
}

// SortRanges sorts Ranges in ascending order by lower bound.
func SortRanges(ranges []Range) {
	sort.Slice(ranges, func(i, j int) bool {
		l, r := ranges[i], ranges[j]
		if r.Start.Unbound {
			return false
		}
		if l.Start.Unbound {
			return true
		}
		td := val.MinTupleDescriptor(l.KeyDesc, r.KeyDesc)
		return td.Compare(l.Start.Key, r.Start.Key) == -1
	})
}

// todo(andy): this doesn't consider inclusivity
func rangesOverlap(left, right Range) bool {
	if left.Stop.Unbound || right.Start.Unbound {
		return true
	}
	td := val.MinTupleDescriptor(left.KeyDesc, right.KeyDesc)
	return td.Compare(left.Stop.Key, right.Start.Key) >= 0
}

func mergeRanges(left, right Range) Range {
	td := val.MinTupleDescriptor(left.KeyDesc, right.KeyDesc)
	return Range{
		Start:   left.Start,
		Stop:    right.Stop,
		KeyDesc: td,
	}
}

type MapRangeIter interface {
	Next(ctx context.Context) (key, value val.Tuple, err error)
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

type rangeIter interface {
	iterate(ctx context.Context) error
	current() (key, value val.Tuple)
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
