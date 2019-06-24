// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "context"

// MapIterator is the interface used by iterators over Noms Maps.
type MapIterator interface {
	Next(ctx context.Context) (k, v Value)
	Prev(ctx context.Context) (k, v Value)
}

// mapIterator can efficiently iterate through a Noms Map.
type mapIterator struct {
	cursor       *sequenceCursor
	currentKey   Value
	currentValue Value
}

// Next returns the subsequent entries from the Map, starting with the entry at which the iterator
// was created. If there are no more entries, Next() returns nils.
func (mi *mapIterator) Next(ctx context.Context) (k, v Value) {
	if mi.cursor.valid() {
		entry := mi.cursor.current().(mapEntry)
		mi.currentKey, mi.currentValue = entry.key, entry.value
		mi.cursor.advance(ctx)
	} else {
		mi.currentKey, mi.currentValue = nil, nil
	}
	return mi.currentKey, mi.currentValue
}

// Prev returns the previous entries from the Map, starting with the entry at which the iterator
// was created. If there are no more entries, prev() returns nils.
func (mi *mapIterator) Prev(ctx context.Context) (k, v Value) {
	if mi.cursor.valid() {
		entry := mi.cursor.current().(mapEntry)
		mi.currentKey, mi.currentValue = entry.key, entry.value
		mi.cursor.retreat(ctx)
	} else {
		mi.currentKey, mi.currentValue = nil, nil
	}
	return mi.currentKey, mi.currentValue
}
