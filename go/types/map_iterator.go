// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
)

// MapIterator can efficiently iterate through a Noms Map.
type MapIterator struct {
	cursor       *sequenceCursor
	CurrentKey   Value
	CurrentValue Value
}

// Next returns the subsequent entries from the Map, starting with the entry at which the iterator
// was created. If there are no more entries, Next() returns nils.
func (mi *MapIterator) Next() (k, v Value) {
	if mi.cursor == nil {
		d.Panic("Cannot use a nil ListIterator")
	}
	if mi.cursor.valid() {
		entry := mi.cursor.current().(mapEntry)
		mi.CurrentKey, mi.CurrentValue = entry.key, entry.value
		mi.cursor.advance()
	} else {
		mi.CurrentKey, mi.CurrentValue = nil, nil
	}
	return mi.CurrentKey, mi.CurrentValue
}
