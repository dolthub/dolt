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
	"sort"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// IsInOrder iterates over every value and validates that they are returned in key order.  This is intended for testing.
func IsInOrder(itr types.EditProvider) (bool, int) {
	prev := itr.Next()

	if prev == nil {
		return true, 0
	}

	count := 1

	for {
		curr := itr.Next()

		if curr == nil {
			break
		} else if curr.Key.Less(types.Format_7_18, prev.Key) {
			return false, count
		}

		count++
		prev = curr
	}

	return true, count
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
		IsInOrder(NewItr(types.Format_7_18, NewKVPCollection(types.Format_7_18, test.kvps)))
		sort.Stable(types.KVPSort{Values: test.kvps, NBF: types.Format_7_18})

		if len(test.kvps) != len(test.expSorted) {
			t.Error("bad length")
		}

		for i := 0; i < len(test.kvps); i++ {
			if !test.kvps[i].Key.Value(ctx).Equals(test.expSorted[i].Key.Value(ctx)) {
				t.Error("value at", i, "does not match expected.")
			}
		}
	}
}
