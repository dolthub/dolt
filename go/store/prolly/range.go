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

type RangeCut struct {
	Key       val.Tuple
	Inclusive bool
	Unbound   bool
}

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

type MapRangeIter struct {
	memCur, proCur tupleCursor
	rng            Range
}

type tupleCursor interface {
	current() (key, value val.Tuple)
	advance(ctx context.Context) error
	retreat(ctx context.Context) error
}

func (it MapRangeIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	for err == nil && value == nil {
		// |value| == nil is a pending delete
		key, value, err = it.nextPair(ctx)
	}
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

func (it MapRangeIter) nextPair(ctx context.Context) (key, value val.Tuple, err error) {
	memKey, proKey := it.currentKeys()

	if memKey == nil && proKey == nil {
		// tuple cursors exhausted
		return nil, nil, io.EOF
	}

	cmp := it.compareKeys(memKey, proKey)

	if cmp >= 0 {
		key, value = it.proCur.current()
		if err = it.progressCursor(ctx, it.proCur); err != nil {
			return nil, nil, err
		}
	}

	// if |cmp| == 0, progress both cursors and
	// use |key| and |value| from memory overlay

	if cmp <= 0 {
		key, value = it.memCur.current()
		if err = it.progressCursor(ctx, it.memCur); err != nil {
			return nil, nil, err
		}
	}

	if !it.rng.insideStop(key) {
		return nil, nil, io.EOF
	}

	return key, value, nil
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

func (it MapRangeIter) compareKeys(memKey, proKey val.Tuple) (cmp int) {
	if memKey == nil {
		cmp = 1
		return
	}
	if proKey == nil {
		cmp = -1
		return
	}

	cmp = it.rng.KeyDesc.Compare(memKey, proKey)
	if it.rng.Reverse {
		cmp = -cmp
	}

	return
}

func (it MapRangeIter) progressCursor(ctx context.Context, cur tupleCursor) error {
	if it.rng.Reverse {
		return cur.retreat(ctx)
	} else {
		return cur.advance(ctx)
	}
}

func startInRange(ctx context.Context, it MapRangeIter) (err error) {
	if it.rng.Start.Unbound {
		return nil
	}

	memKey, proKey := it.currentKeys()
	if memKey == nil && proKey == nil {
		return nil // tuple cursors exhausted
	}

	var key, value val.Tuple

	if it.compareKeys(memKey, proKey) > 0 {
		key, value = it.proCur.current()
	} else {
		key, value = it.memCur.current()
	}

	for value == nil || !it.rng.insideStart(key) {
		// |value| == nil is a pending delete

		cmp := it.compareKeys(memKey, proKey)
		if cmp >= 0 {
			if err = it.progressCursor(ctx, it.proCur); err != nil {
				return err
			}
			proKey, _ = it.proCur.current()
		}
		if cmp <= 0 {
			if err = it.progressCursor(ctx, it.memCur); err != nil {
				return err
			}
			memKey, _ = it.memCur.current()
		}

		if it.compareKeys(memKey, proKey) > 0 {
			key, value = current(it.proCur)
		} else {
			key, value = current(it.memCur)
		}

		if key == nil {
			break
		}
	}

	// both cursors are now within |it.rng|
	return nil
}

func current(tc tupleCursor) (key, value val.Tuple) {
	if tc != nil {
		key, value = tc.current()
	}
	return
}
