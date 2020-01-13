// Copyright 2019 Liquidata, Inc.
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

import "context"

type ForwardMapIterator interface {
	Next(ctx context.Context) (k, v Value, err error)
}

// MapIterator is the interface used by iterators over Noms Maps.
type MapIterator interface {
	ForwardMapIterator
	Prev(ctx context.Context) (k, v Value, err error)
}

// mapIterator can efficiently iterate through a Noms Map.
type mapIterator struct {
	cursor       *sequenceCursor
	currentKey   Value
	currentValue Value
}

// Next returns the subsequent entries from the Map, starting with the entry at which the iterator
// was created. If there are no more entries, Next() returns nils.
func (mi *mapIterator) Next(ctx context.Context) (k, v Value, err error) {
	if mi.cursor.valid() {
		item, err := mi.cursor.current()

		if err != nil {
			return nil, nil, err
		}

		entry := item.(mapEntry)
		mi.currentKey, mi.currentValue = entry.key, entry.value
		_, err = mi.cursor.advance(ctx)

		if err != nil {
			return nil, nil, err
		}
	} else {
		mi.currentKey, mi.currentValue = nil, nil
	}

	return mi.currentKey, mi.currentValue, nil
}

// Prev returns the previous entries from the Map, starting with the entry at which the iterator
// was created. If there are no more entries, prev() returns nils.
func (mi *mapIterator) Prev(ctx context.Context) (k, v Value, err error) {
	if mi.cursor.valid() {
		item, err := mi.cursor.current()

		if err != nil {
			return nil, nil, err
		}

		entry := item.(mapEntry)
		mi.currentKey, mi.currentValue = entry.key, entry.value
		_, err = mi.cursor.retreat(ctx)

		if err != nil {
			return nil, nil, err
		}
	} else {
		mi.currentKey, mi.currentValue = nil, nil
	}

	return mi.currentKey, mi.currentValue, nil
}

func NewBufferedMapIterator(ctx context.Context, m Map) (ForwardMapIterator, error) {
	bufSeqCur, err := newBufferedSequenceCursor(ctx, m.asSequence(), 64*64*64)

	if err != nil {
		return nil, err
	}

	return &bufferedMapIterator{bufSeqCur, nil, nil}, nil
}

type bufferedMapIterator struct {
	bufSeqCur    *bufSeqCurImpl
	currentKey   Value
	currentValue Value
}

func (bmi *bufferedMapIterator) Next(ctx context.Context) (k, v Value, err error) {
	if bmi.bufSeqCur.valid() {
		item, err := bmi.bufSeqCur.current()

		if err != nil {
			return nil, nil, err
		}

		entry := item.(mapEntry)
		bmi.currentKey, bmi.currentValue = entry.key, entry.value
		_, err = bmi.bufSeqCur.advance(ctx)

		if err != nil {
			return nil, nil, err
		}
	} else {
		bmi.currentKey, bmi.currentValue = nil, nil
	}

	return bmi.currentKey, bmi.currentValue, nil
}
