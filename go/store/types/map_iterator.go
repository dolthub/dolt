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
	mapEntryChan := make(chan mapEntry, bufferSize)
	errChan := make(chan error)

	// process the map and populate the buffer
	go func() {
		defer close(mapEntryChan)
		//defer close(errChan)

		err := m.IterAll(ctx, func(key, value Value) error {
			mapEntryChan <- mapEntry{key, value}
			return nil
		})
		if err != nil {
			errChan <- err
		}
	}()

	return &bufferedMapIterator{mapEntryChan, errChan}, nil
}

// todo: calculate buffer size to be a number of bytes
const bufferSize = 100 * 1000
type bufferedMapIterator struct {
	entryStream <-chan mapEntry
	errChan 	<-chan error
}

func (bmi *bufferedMapIterator) Next(ctx context.Context) (k, v Value, err error) {
	select {
	case e := <-bmi.errChan:
		return nil, nil, e
	case me := <-bmi.entryStream:
		return me.key, me.value, nil
	}
}
