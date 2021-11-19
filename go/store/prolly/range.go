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
	Reverse     bool
}

func (r Range) insideStart(key val.Tuple) bool {
	if r.Start.Unbound {
		return true
	}

	cmp := r.KeyDesc.Compare(key, r.Start.Key)
	if cmp == 0 {
		return r.Start.Inclusive
	}
	if r.Reverse {
		cmp = -cmp
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
	if r.Reverse {
		cmp = -cmp
	}
	return cmp < 0
}

// GreaterRange defines a Range of Tuples greater than |start|.
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
		Reverse: false,
	}
}

// GreaterOrEqualRange defines a Range of Tuples greater than or equal to |start|.
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
		Reverse: false,
	}
}

// LesserRange defines a Range of Tuples less than |stop|.
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
		Reverse: false,
	}
}

// LesserOrEqualRange defines a Range of Tuples less than or equal to |stop|.
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
		Reverse: false,
	}
}

// todo(andy): reverse ranges for GT, GTE, LT, and LTE?

// OpenRange defines a non-inclusive Range of Tuples from |start| to |stop|.
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
		Reverse: desc.Compare(start, stop) > 0,
	}
}

// OpenStartRange defines a half-open Range of Tuples from |start| to |stop|.
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
		Reverse: desc.Compare(start, stop) > 0,
	}
}

// OpenStopRange defines a half-open Range of Tuples from |start| to |stop|.
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
		Reverse: desc.Compare(start, stop) > 0,
	}
}

// ClosedRange defines an inclusive Range of Tuples from |start| to |stop|.
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
		Reverse: desc.Compare(start, stop) > 0,
	}
}

// MapRangeIter iterates over a Range of Tuples.
type MapRangeIter struct {
	memCur, proCur tupleCursor
	rng            Range
}

func NewMapRangeIter(ctx context.Context, memCur, proCur tupleCursor, rng Range) (MapRangeIter, error) {
	mri := MapRangeIter{
		memCur: memCur,
		proCur: proCur,
		rng:    rng,
	}

	err := startInRange(ctx, mri)
	if err != nil {
		return MapRangeIter{}, err
	}

	return mri, nil
}

type tupleCursor interface {
	current() (key, value val.Tuple)
	advance(ctx context.Context) error
	retreat(ctx context.Context) error
}

// Next returns the next pair of Tuples in the Range, or io.EOF if the iter is done.
func (it MapRangeIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	for value == nil {
		key, value = it.current()

		if key == nil {
			// |key| == nil is exhausted |iter|
			return nil, nil, io.EOF
		}

		// |value| == nil is a pending delete

		if err = it.progress(ctx); err != nil {
			return nil, nil, err
		}
	}

	return key, value, nil
}

func (it MapRangeIter) current() (key, value val.Tuple) {
	memKey, proKey := it.currentKeys()

	if memKey == nil && proKey == nil {
		// cursors exhausted
		return nil, nil
	}

	if it.compareKeys(memKey, proKey) > 0 {
		key, value = it.proCur.current()
	} else {
		// |memCur| wins ties
		key, value = it.memCur.current()
	}

	if !it.rng.insideStop(key) {
		return nil, nil
	}

	return
}

func (it MapRangeIter) currentKeys() (memKey, proKey val.Tuple) {
	if it.memCur != nil {
		memKey, _ = it.memCur.current()
	}
	if it.proCur != nil {
		proKey, _ = it.proCur.current()
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

	cmp := it.rng.KeyDesc.Compare(memKey, proKey)
	if it.rng.Reverse {
		cmp = -cmp
	}
	return cmp
}

func (it MapRangeIter) progress(ctx context.Context) (err error) {
	memKey, proKey := it.currentKeys()

	if memKey == nil && proKey == nil {
		return nil // can't progress
	}

	cmp := it.compareKeys(memKey, proKey)

	if cmp >= 0 {
		if err = it.moveCursor(ctx, it.proCur); err != nil {
			return err
		}
	}
	if cmp <= 0 {
		if err = it.moveCursor(ctx, it.memCur); err != nil {
			return err
		}
	}
	return
}

func (it MapRangeIter) moveCursor(ctx context.Context, cur tupleCursor) error {
	if it.rng.Reverse {
		return cur.retreat(ctx)
	} else {
		return cur.advance(ctx)
	}
}

func startInRange(ctx context.Context, iter MapRangeIter) error {
	if iter.rng.Start.Unbound {
		return nil
	}

	key, value := iter.current()
	if key == nil {
		// |key| == nil is exhausted iter
		return nil
	}

	// |value| == nil is a pending delete

	for !iter.rng.insideStart(key) || value == nil {
		if err := iter.progress(ctx); err != nil {
			return err
		}

		key, value = iter.current()
		if key == nil {
			// |key| == nil is exhausted iter
			return nil
		}
	}

	return nil
}
