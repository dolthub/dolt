// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"errors"
	"io"
)

// MapTupleIterator is an iterator that returns map keys and values as types.Tuple instances and follow the standard go
// convention of using io.EOF to mean that all the data has been read.
type MapTupleIterator interface {
	NextTuple(ctx context.Context) (k, v Tuple, err error)
}

// MapIterator is the interface used by iterators over Noms Maps.
type MapIterator interface {
	MapTupleIterator
	Next(ctx context.Context) (k, v Value, err error)
}

// mapIterator can efficiently iterate through a Noms Map.
type mapIterator struct {
	sequenceIter sequenceIterator
}

// Next returns the subsequent entries from the Map, starting with the entry at which the iterator
// was created. If there are no more entries, Next() returns nils.
func (mi *mapIterator) Next(ctx context.Context) (k, v Value, err error) {
	if mi.sequenceIter.valid() {
		item, err := mi.sequenceIter.current()

		if err != nil {
			return nil, nil, err
		}

		entry := item.(mapEntry)
		_, err = mi.sequenceIter.advance(ctx)

		if err != nil {
			return nil, nil, err
		}

		return entry.key, entry.value, nil
	} else {
		return nil, nil, nil
	}
}

// Next returns the subsequent entries from the Map, starting with the entry at which the iterator
// was created. If there are no more entries, Next() returns nils.
func (mi *mapIterator) NextTuple(ctx context.Context) (k, v Tuple, err error) {
	if mi.sequenceIter.valid() {
		entry, err := mi.sequenceIter.currentTuple()

		if err != nil {
			return Tuple{}, Tuple{}, err
		}

		_, err = mi.sequenceIter.advance(ctx)

		if err != nil {
			return Tuple{}, Tuple{}, err
		}

		return entry.key, entry.value, nil
	} else {
		return Tuple{}, Tuple{}, io.EOF
	}
}

var errClosed = errors.New("closed")

type mapRangeIter struct {
	collItr *collTupleRangeIter
}

func (itr *mapRangeIter) NextTuple(ctx context.Context) (k, v Tuple, err error) {
	if itr.collItr == nil {
		// only happens if there is nothing to iterate over
		return Tuple{}, Tuple{}, io.EOF
	}

	k, err = itr.collItr.Next()

	if err != nil {
		return Tuple{}, Tuple{}, err
	}

	v, err = itr.collItr.Next()

	if err != nil {
		return Tuple{}, Tuple{}, err
	}

	return k, v, nil
}

func (m Map) RangeIterator(ctx context.Context, startIdx, endIdx uint64) (MapTupleIterator, error) {
	// newCollRangeItr returns nil if the number of elements being iterated over is 0
	collItr, err := newCollRangeIter(ctx, m, startIdx, endIdx)

	if err != nil {
		return nil, err
	}

	return &mapRangeIter{collItr: collItr}, nil
}
