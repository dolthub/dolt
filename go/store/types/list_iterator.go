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
func (li ListIterator) Next(ctx context.Context) (out Value) {
	if li.cursor == nil {
		d.Panic("Cannot use a nil ListIterator")
	}
	if li.cursor.valid() {
		out = li.cursor.current().(Value)
		li.cursor.advance(ctx)
	}
	return
}
