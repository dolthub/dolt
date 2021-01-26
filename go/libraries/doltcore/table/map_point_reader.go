// Copyright 2020 Dolthub, Inc.
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

package table

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/types"
)

type PointReader struct {
	m          types.Map
	emptyTuple types.Tuple
	keys       []types.Tuple
	idx        int
}

var _ types.MapIterator = &PointReader{}

// read the map values for a set of map keys
func NewMapPointReader(m types.Map, keys ...types.Tuple) types.MapIterator {
	return &PointReader{
		m:          m,
		emptyTuple: types.EmptyTuple(m.Format()),
		keys:       keys,
	}
}

// Next implements types.MapIterator.
func (pr *PointReader) Next(ctx context.Context) (k, v types.Value, err error) {
	kt, vt, err := pr.NextTuple(ctx)

	if err != nil {
		return nil, nil, err
	}

	if !kt.Empty() {
		k = kt
	}

	if !vt.Empty() {
		v = vt
	}

	return k, v, nil
}

// Next implements types.MapIterator.
func (pr *PointReader) NextTuple(ctx context.Context) (k, v types.Tuple, err error) {
	if pr.idx >= len(pr.keys) {
		return types.Tuple{}, types.Tuple{}, io.EOF
	}

	k = pr.keys[pr.idx]
	v = pr.emptyTuple

	var ok bool
	// todo: optimize by implementing MapIterator.Seek()
	v, ok, err = pr.m.MaybeGetTuple(ctx, k)
	pr.idx++

	if err != nil {
		return types.Tuple{}, types.Tuple{}, err
	} else if !ok {
		return k, pr.emptyTuple, nil
	}

	return k, v, nil
}
