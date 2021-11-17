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

type Range struct {
	Start, Stop RangeCut
	Reverse     bool
}

type RangeCut struct {
	Key       val.Tuple
	Inclusive bool
	Unbound   bool
}

type tupleCursor interface {
	current() (key, value val.Tuple)
	advance(ctx context.Context) error
	retreat(ctx context.Context) error
	desc() val.TupleDesc
}

func iterRange(ctx context.Context, r Range, cur tupleCursor) (key, value val.Tuple, err error) {
	key, value = cur.current()
	if key == nil {
		return nil, nil, io.EOF
	}

	if !r.Stop.Unbound { // check bounds
		cmp := cur.desc().Compare(key, r.Stop.Key)

		if cmp == 0 && !r.Stop.Inclusive {
			return nil, nil, io.EOF
		}
		if r.Reverse {
			cmp *= -1
		}
		if cmp > 0 {
			return nil, nil, io.EOF
		}
	}

	if r.Reverse {
		err = cur.retreat(ctx)
	} else {
		err = cur.advance(ctx)
	}

	return
}

// assumes we're no more than one position away from the correct starting position.
func startInsideRange(ctx context.Context, r Range, cur tupleCursor) error {
	if r.Start.Unbound {
		return nil
	}

	key, _ := cur.current()
	if key == nil {
		return io.EOF
	}
	cmp := cur.desc().Compare(key, r.Start.Key)

	if cmp == 0 && r.Start.Inclusive {
		return nil
	}

	if r.Reverse && cmp >= 0 {
		return cur.retreat(ctx)
	}

	if !r.Reverse && cmp <= 0 {
		return cur.advance(ctx)
	}

	return nil
}
