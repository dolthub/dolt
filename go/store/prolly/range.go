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

type Range struct {
	Start, Stop RangeCut
	KeyDesc     val.TupleDesc
	Reverse     bool
}

type RangeCut struct {
	Key       val.Tuple
	Inclusive bool
	Unbound   bool
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
	startInRange(ctx context.Context, r Range) error
}

func (it MapRangeIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	memKey, proKey := it.currentKeys()

	if memKey == nil && proKey == nil {
		return nil, nil, io.EOF
	}

	goMem, goPro := it.compareCursors(memKey, proKey)

	if goPro {
		key, value = it.proCur.current()
		err = it.progressCursor(ctx, it.proCur)
		if err != nil {
			return nil, nil, err
		}
	}

	// if |goPro| and |goMem| are both true, advance both
	// cursors and take |key| and |value| from |memCur|
	if goMem {
		key, value = it.memCur.current()
		err = it.progressCursor(ctx, it.memCur)
		if err != nil {
			return nil, nil, err
		}
	}

	return key, value, nil
}

func (it MapRangeIter) currentKeys() (memKey, proKey val.Tuple) {
	if it.memCur != nil {
		memKey, _ = it.memCur.current()
		if memKey != nil && !it.keyInRange(memKey) {
			memKey = nil
		}
	}
	if it.proCur != nil {
		proKey, _ = it.proCur.current()
		if proKey != nil && !it.keyInRange(proKey) {
			proKey = nil
		}
	}
	return
}

func (it MapRangeIter) compareCursors(memKey, proKey val.Tuple) (goMem, goPro bool) {
	if memKey == nil {
		goPro = true
		return
	}

	if proKey == nil {
		goMem = true
		return
	}

	cmp := it.rng.KeyDesc.Compare(memKey, proKey)
	if it.rng.Reverse {
		cmp = -cmp
	}
	if cmp <= 0 {
		goMem = true
	}
	if cmp >= 0 {
		goPro = true
	}

	return
}

func (it MapRangeIter) keyInRange(key val.Tuple) bool {
	if key == nil {
		panic("nil key")
	}
	if it.rng.Stop.Unbound {
		return true
	}

	cmp := it.rng.KeyDesc.Compare(key, it.rng.Stop.Key)
	if it.rng.Reverse {
		cmp = -cmp
	}

	if cmp == 0 {
		return it.rng.Stop.Inclusive
	}

	return cmp < 0
}

func (it MapRangeIter) progressCursor(ctx context.Context, cur tupleCursor) error {
	if it.rng.Reverse {
		return cur.retreat(ctx)
	} else {
		return cur.advance(ctx)
	}
}
