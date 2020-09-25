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

package edits

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

// IsInOrder iterates over every value and validates that they are returned in key order.  This is intended for testing.
func IsInOrder(itr types.EditProvider) (bool, int, error) {
	prev, err := itr.Next()

	if err != nil {
		return false, 0, err
	}

	if prev == nil {
		return true, 0, nil
	}

	count := 1

	for {
		curr, err := itr.Next()

		if err != nil {
			return false, 0, err
		}

		if curr == nil {
			break
		} else {
			isLess, err := curr.Key.Less(types.Format_7_18, prev.Key)

			if err != nil {
				return false, 0, err
			}

			if isLess {
				return false, count, nil
			}
		}

		count++
		prev = curr
	}

	return true, count, nil
}

func TestKVPSliceSort(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		kvps      types.KVPSlice
		expSorted types.KVPSlice
	}{
		{
			types.KVPSlice{
				{Key: types.Uint(5), Val: types.NullValue},
				{Key: types.Uint(1), Val: types.NullValue},
				{Key: types.Uint(4), Val: types.NullValue},
				{Key: types.Uint(3), Val: types.NullValue},
			},
			types.KVPSlice{
				{Key: types.Uint(1), Val: types.NullValue},
				{Key: types.Uint(3), Val: types.NullValue},
				{Key: types.Uint(4), Val: types.NullValue},
				{Key: types.Uint(5), Val: types.NullValue},
			},
		},
	}

	for _, test := range tests {
		_, _, err := IsInOrder(NewItr(types.Format_7_18, NewKVPCollection(types.Format_7_18, test.kvps)))
		assert.NoError(t, err)
		err = types.SortWithErroringLess(types.KVPSort{Values: test.kvps, NBF: types.Format_7_18})
		assert.NoError(t, err)

		if len(test.kvps) != len(test.expSorted) {
			t.Error("bad length")
		}

		for i := 0; i < len(test.kvps); i++ {
			val, err := test.kvps[i].Key.Value(ctx)
			assert.NoError(t, err)
			expVal, err := test.expSorted[i].Key.Value(ctx)
			assert.NoError(t, err)
			if !val.Equals(expVal) {
				t.Error("value at", i, "does not match expected.")
			}
		}
	}
}
