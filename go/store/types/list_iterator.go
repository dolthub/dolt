// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
)

// ListIterator can be used to efficiently iterate through a Noms List.
type ListIterator struct {
	cursor *sequenceCursor
}

// Next returns subsequent Values from a List, starting with the index at which the iterator was
// created. If there are no more Values, Next() returns nil.
func (li ListIterator) Next(ctx context.Context) (Value, error) {
	if li.cursor == nil {
		d.Panic("Cannot use a nil ListIterator")
	}

	var out Value
	if li.cursor.valid() {
		currItem, err := li.cursor.current()

		if err != nil {
			return nil, err
		}

		out = currItem.(Value)

		_, err = li.cursor.advance(ctx)

		if err != nil {
			return nil, err
		}
	}

	return out, nil
}
