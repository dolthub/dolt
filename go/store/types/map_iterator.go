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

	"golang.org/x/sync/errgroup"
)

// MapIterator is the interface used by iterators over Noms Maps.
type MapIterator interface {
	Next(ctx context.Context) (k, v Value, err error)
}

// mapIterator can efficiently iterate through a Noms Map.
type mapIterator struct {
	sequenceIter sequenceIterator
	currentKey   Value
	currentValue Value
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
		mi.currentKey, mi.currentValue = entry.key, entry.value
		_, err = mi.sequenceIter.advance(ctx)

		if err != nil {
			return nil, nil, err
		}
	} else {
		mi.currentKey, mi.currentValue = nil, nil
	}

	return mi.currentKey, mi.currentValue, nil
}

type mapKeyValuePair struct {
	k Value
	v Value
}

var errClosed = errors.New("closed")

type readAheadRangeIter struct {
	ctx  context.Context
	eg   *errgroup.Group
	kvCh chan mapKeyValuePair
}

func (itr *readAheadRangeIter) Next(context.Context) (Value, Value, error) {
	select {
	case kvp, ok := <-itr.kvCh:
		if !ok {
			return nil, nil, nil
		}

		return kvp.k, kvp.v, nil

	case <-itr.ctx.Done():
		err := itr.eg.Wait()

		if err != errClosed {
			return nil, nil, err
		}

		return nil, nil, nil
	}
}

func (itr *readAheadRangeIter) Close() error {
	itr.eg.Go(func() error {
		return errClosed
	})

	_ = itr.eg.Wait()
	close(itr.kvCh)
	return nil
}

func (m Map) RangeIterator(ctx context.Context, readAhead int, startIdx, endIdx uint64) (MapIterator, error) {
	eg, ctx := errgroup.WithContext(ctx)
	keyValCh := make(chan mapKeyValuePair, readAhead)
	eg.Go(func() error {
		err := m.IterRange(ctx, startIdx, endIdx, func(key, value Value) error {
			kvp := mapKeyValuePair{key, value}
			select {
			case keyValCh <- kvp:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})

		if err != nil {
			return err
		}

		// send an empty kvp to signify the end of the range
		kvp := mapKeyValuePair{}
		select {
		case keyValCh <- kvp:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	return &readAheadRangeIter{ctx, eg, keyValCh}, nil
}
