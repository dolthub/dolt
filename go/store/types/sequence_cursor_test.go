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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

type testSequence struct {
	items []interface{}
}

// sequence interface
func (ts testSequence) getItem(idx int) (sequenceItem, error) {
	return ts.items[idx], nil
}

func (ts testSequence) seqLen() int {
	return len(ts.items)
}

func (ts testSequence) numLeaves() uint64 {
	return uint64(len(ts.items))
}

func (ts testSequence) cumulativeNumberOfLeaves(idx int) (uint64, error) {
	panic("not reached")
}

func (ts testSequence) getCompositeChildSequence(ctx context.Context, start uint64, length uint64) (sequence, error) {
	panic("not reached")
}

func (ts testSequence) treeLevel() uint64 {
	panic("not reached")
}

func (ts testSequence) Kind() NomsKind {
	panic("not reached")
}

func (ts testSequence) getCompareFn(other sequence) compareFn {
	obl := other.(testSequence)
	return func(idx, otherIdx int) (bool, error) {
		return ts.items[idx] == obl.items[otherIdx], nil
	}
}

func (ts testSequence) valueReadWriter() ValueReadWriter {
	panic("not reached")
}

func (ts testSequence) writeTo(nomsWriter, *NomsBinFormat) error {
	panic("not reached")
}

func (ts testSequence) format() *NomsBinFormat {
	return Format_Default
}

func (ts testSequence) getChildSequence(_ context.Context, idx int) (sequence, error) {
	child := ts.items[idx]
	childSlice, ok := child.([]interface{})
	if !ok {
		return nil, nil
	}
	return testSequence{childSlice}, nil
}

func (ts testSequence) isLeaf() bool {
	panic("not reached")
}

func (ts testSequence) Equals(other Value) bool {
	panic("not reached")
}

func (ts testSequence) valuesSlice(from, to uint64) ([]Value, error) {
	panic("not reached")
}

func (ts testSequence) kvTuples(from, to uint64, dest []Tuple) ([]Tuple, error) {
	panic("not reached")
}

func (ts testSequence) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	panic("not reached")
}

func (ts testSequence) Compare(nbf *NomsBinFormat, other LesserValuable) (int, error) {
	panic("not reached")
}

func (ts testSequence) Hash(*NomsBinFormat) (hash.Hash, error) {
	panic("not reached")
}

func (ts testSequence) walkRefs(nbf *NomsBinFormat, cb RefCallback) error {
	panic("not reached")
}

func (ts testSequence) typeOf() (*Type, error) {
	panic("not reached")
}

func (ts testSequence) Len() uint64 {
	return uint64(len(ts.items))
}

func (ts testSequence) Empty() bool {
	panic("not reached")
}

func (ts testSequence) asValueImpl() valueImpl {
	panic("not reached")
}

func newTestSequenceCursor(items []interface{}) *sequenceCursor {
	parent := newSequenceCursor(nil, testSequence{items}, 0)
	items = items[0].([]interface{})
	return newSequenceCursor(parent, testSequence{items}, 0)
}

func TestTestCursor(t *testing.T) {
	assert := assert.New(t)

	var cur *sequenceCursor
	reset := func() {
		cur = newTestSequenceCursor([]interface{}{[]interface{}{100, 101}, []interface{}{102}})
	}
	expect := func(expectIdx, expectParentIdx int, expectOk bool, expectVal sequenceItem) {
		assert.Equal(expectIdx, cur.indexInChunk())
		assert.Equal(expectParentIdx, cur.parent.indexInChunk())
		assert.Equal(expectOk, cur.valid())
		if cur.valid() {
			seq, err := cur.current()
			require.NoError(t, err)
			assert.Equal(expectVal, seq)
		}
	}

	// Test retreating past the start.
	reset()
	expect(0, 0, true, sequenceItem(100))
	assert.False(cur.retreat(context.Background()))
	expect(-1, 0, false, nil)
	assert.False(cur.retreat(context.Background()))
	expect(-1, 0, false, nil)

	// Test retreating past the start, then advanding past the end.
	reset()
	assert.False(cur.retreat(context.Background()))
	assert.True(cur.advance(context.Background()))
	expect(0, 0, true, sequenceItem(100))
	assert.True(cur.advance(context.Background()))
	expect(1, 0, true, sequenceItem(101))
	assert.True(cur.advance(context.Background()))
	expect(0, 1, true, sequenceItem(102))
	assert.False(cur.advance(context.Background()))
	expect(1, 1, false, nil)
	assert.False(cur.advance(context.Background()))
	expect(1, 1, false, nil)

	// Test advancing past the end.
	reset()
	assert.True(cur.advance(context.Background()))
	expect(1, 0, true, sequenceItem(101))
	assert.True(cur.retreat(context.Background()))
	expect(0, 0, true, sequenceItem(100))
	assert.False(cur.retreat(context.Background()))
	expect(-1, 0, false, nil)
	assert.False(cur.retreat(context.Background()))
	expect(-1, 0, false, nil)

	// Test advancing past the end, then retreating past the start.
	reset()
	assert.True(cur.advance(context.Background()))
	assert.True(cur.advance(context.Background()))
	expect(0, 1, true, sequenceItem(102))
	assert.False(cur.advance(context.Background()))
	expect(1, 1, false, nil)
	assert.False(cur.advance(context.Background()))
	expect(1, 1, false, nil)
	assert.True(cur.retreat(context.Background()))
	expect(0, 1, true, sequenceItem(102))
	assert.True(cur.retreat(context.Background()))
	expect(1, 0, true, sequenceItem(101))
	assert.True(cur.retreat(context.Background()))
	expect(0, 0, true, sequenceItem(100))
	assert.False(cur.retreat(context.Background()))
	expect(-1, 0, false, nil)
	assert.False(cur.retreat(context.Background()))
	expect(-1, 0, false, nil)
}
