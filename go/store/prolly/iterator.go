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

type MapIter interface {
	Next(ctx context.Context) (key, value val.Tuple, err error)
}

type ValueRange struct {
	lowKey, highKey val.Tuple
	inclusiveLow    bool
	inclusiveHigh   bool
	reverse         bool
}

type valueIter struct {
	rng ValueRange
	cur nodeCursor
}

func (it *valueIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	panic("unimplemented")
}

// IndexRange is an inclusive range of item indexes
type IndexRange struct {
	low, high uint64
	reverse   bool
}

type indexIter struct {
	rng IndexRange
	cur nodeCursor
	rem uint64
}

func (it *indexIter) Next(ctx context.Context) (key, value val.Tuple, err error) {
	if it.rem == 0 {
		return nil, nil, io.EOF
	}

	pair := it.cur.currentPair()
	key, value = val.Tuple(pair.key()), val.Tuple(pair.value())

	if it.rng.reverse {
		_, err = it.cur.retreat(ctx)
	} else {
		_, err = it.cur.advance(ctx)
	}
	if err != nil {
		return nil, nil, err
	}

	it.rem--
	return
}
