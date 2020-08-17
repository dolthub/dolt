// Copyright 2020 Liquidata, Inc.
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

package types

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type testOrderedSequence struct {
	testSequence
}

func (t *testOrderedSequence) getKey(idx int) (orderedKey, error) {
	cs, err := t.getChildSequence(nil, idx)
	if err != nil {
		return orderedKey{}, err
	}
	if cs != nil {
		return cs.(*testOrderedSequence).getKey(cs.seqLen()-1)
	}

	return newOrderedKey(t.items[idx].(Value), Format_Default)
}

func (t *testOrderedSequence) getChildSequence(_ context.Context, idx int) (sequence, error) {
	child := t.items[idx]
	childSeq, ok := child.(*testOrderedSequence)
	if !ok {
		return nil, nil
	}
	return childSeq, nil
}

func (t *testOrderedSequence) search(key orderedKey) (int, error) {
	idx, err := SearchWithErroringLess(int(t.Len()), func(i int) (bool, error) {
		k, err := t.getKey(i)

		if err != nil {
			return false, err
		}

		isLess, err := k.Less(t.format(), key)

		if err != nil {
			return false, nil
		}

		return !isLess, nil
	})

	return idx, err
}

// items is a slice of slices of slices... of Values. Each slice that contains non-value children will be treated as the
// parent slice for N additional children, one for each slice.
func newOrderedTestSequence(items []interface{}) *testOrderedSequence {
	_, firstChildIsValue := items[0].(Value)
	if firstChildIsValue {
		return &testOrderedSequence{
			testSequence: testSequence{items},
		}
	}

	var sequenceItems []interface{}
	for _, item := range items {
		sequenceItems = append(sequenceItems, newOrderedTestSequence(item.([]interface{})))
	}

	return &testOrderedSequence{
		testSequence: testSequence{sequenceItems},
	}
}

type orderedSequenceTestCase struct {
	value        Int
	expectedVals []Int
}

func newOrderedSequenceTestCase(val int, expectedValues ...int) orderedSequenceTestCase {
	expected := make([]Int, len(expectedValues))
	for i, value := range expectedValues {
		expected[i] = Int(value)
	}
	return orderedSequenceTestCase{
		value:        Int(val),
		expectedVals: expected,
	}
}

func TestNewCursorAtValue(t *testing.T) {
	testSequence := newOrderedTestSequence([]interface{}{
		[]interface{}{
			[]interface{}{
				Int(1),
				Int(2),
			},
			[]interface{}{
				Int(4),
				Int(5),
				Int(7),
			},
		},
		[]interface{}{
			[]interface{}{
				Int(10),
				Int(11),
			},
			[]interface{}{
				Int(20),
			},
		},
	})

	testCases := []orderedSequenceTestCase{
		newOrderedSequenceTestCase(0, 1, 2, 4, 5, 7, 10, 11, 20),
		newOrderedSequenceTestCase(1, 1, 2, 4, 5, 7, 10, 11, 20),
		newOrderedSequenceTestCase(4, 4, 5, 7, 10, 11, 20),
		newOrderedSequenceTestCase(6, 7, 10, 11, 20),
		newOrderedSequenceTestCase(7, 7, 10, 11, 20),
		newOrderedSequenceTestCase(8, 10, 11, 20),
		newOrderedSequenceTestCase(10, 10, 11, 20),
		newOrderedSequenceTestCase(12, 20),
		newOrderedSequenceTestCase(20, 20),
		newOrderedSequenceTestCase(21),
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%d", tt.value), func(t *testing.T) {
			cursor, err := newCursorAtValue(context.Background(), testSequence, tt.value, false, false)
			require.NoError(t, err)
			assertCursorContents(t, cursor, tt.expectedVals)
		})
	}
}

func TestNewCursorBackFromValue(t *testing.T) {
	testSequence := newOrderedTestSequence([]interface{}{
		[]interface{}{
			[]interface{}{
				Int(1),
				Int(2),
			},
			[]interface{}{
				Int(4),
				Int(5),
				Int(7),
			},
		},
		[]interface{}{
			[]interface{}{
				Int(10),
				Int(11),
			},
			[]interface{}{
				Int(20),
			},
		},
	})

	// TODO: figure out what the semantics for the first element of non-present key searches should be
	testCases := []orderedSequenceTestCase{
		newOrderedSequenceTestCase(0),
		newOrderedSequenceTestCase(1, 1),
		newOrderedSequenceTestCase(4, 4, 2, 1),
		newOrderedSequenceTestCase(6, 5, 4, 2, 1),
		newOrderedSequenceTestCase(7, 7, 5, 4, 2, 1),
		newOrderedSequenceTestCase(8, 7, 5, 4, 2, 1),
		newOrderedSequenceTestCase(10, 10, 7, 5, 4, 2, 1),
		newOrderedSequenceTestCase(11, 11, 10, 7, 5, 4, 2, 1),
		newOrderedSequenceTestCase(12, 11, 10, 7, 5, 4, 2, 1),
		newOrderedSequenceTestCase(20, 20, 11, 10, 7, 5, 4, 2, 1),
		newOrderedSequenceTestCase(21, 20, 11, 10, 7, 5, 4, 2, 1),
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("%d", tt.value), func(t *testing.T) {
			cursor, err := newCursorBackFromValue(context.Background(), testSequence, tt.value)
			require.NoError(t, err)
			assertCursorContents(t, cursor, tt.expectedVals)
		})
	}
}

func assertCursorContents(t *testing.T, cursor *sequenceCursor, expectedVals []Int) {
	more := true
	vals := make([]Int, 0)

	for {
		if !cursor.valid() {
			break
		}
		item, err := cursor.current()
		require.NoError(t, err)
		intVal, ok := item.(Int)
		require.True(t, ok)
		vals = append(vals, intVal)

		more, err = cursor.advance(context.Background())
		require.NoError(t, err)
		if !more {
			break
		}
	}

	assert.Equal(t, expectedVals, vals)
}