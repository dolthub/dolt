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

package edits

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

// IsInOrder iterates over every value and validates that they are returned in key order.  This is intended for testing.
func IsInOrder(nbf *types.NomsBinFormat, itr types.EditProvider) (bool, int, error) {
	prev, err := itr.Next()

	if err == io.EOF {
		return true, 0, nil
	} else if err != nil {
		return false, 0, err
	}

	count := 1

	for {
		curr, err := itr.Next()

		if err == io.EOF {
			return true, count, nil
		} else if err != nil {
			return false, 0, err
		}

		isLess, err := curr.Key.Less(nbf, prev.Key)

		if err != nil {
			return false, 0, err
		}

		if isLess {
			return false, count, nil
		}

		count++
		prev = curr
	}
}

func TestKVPSliceSort(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default

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
		_, _, err := IsInOrder(nbf, NewItr(nbf, NewKVPCollection(nbf, test.kvps)))
		assert.NoError(t, err)
		err = types.SortWithErroringLess(types.KVPSort{Values: test.kvps, NBF: nbf})
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
