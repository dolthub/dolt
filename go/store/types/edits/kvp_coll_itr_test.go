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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/types"
)

func TestKVPCollItr(t *testing.T) {
	ctx := context.Background()

	slice1 := types.KVPSlice{{Key: types.Uint(1), Val: types.NullValue}, {Key: types.Uint(2), Val: types.NullValue}}
	slice2 := types.KVPSlice{{Key: types.Uint(3), Val: types.NullValue}, {Key: types.Uint(4), Val: types.NullValue}}
	slice3 := types.KVPSlice{{Key: types.Uint(5), Val: types.NullValue}, {}}

	type itrRes struct {
		keyVal       uint
		exhaustedBuf bool
		done         bool
	}
	tests := []struct {
		buffSize   int
		totalSize  int64
		slices     []types.KVPSlice
		itrResults []itrRes
	}{
		{
			2,
			5,
			[]types.KVPSlice{slice1, slice2, slice3[:1]},
			[]itrRes{
				{1, false, false},
				{2, true, false},
				{3, false, false},
				{4, true, false},
				{5, true, true},
			},
		},
	}

	nbf := types.Format_Default

	for _, test := range tests {
		coll := &KVPCollection{test.buffSize, len(test.slices), test.totalSize, test.slices, nbf}
		itr := NewItr(nbf, coll)

		for i := 0; i < 2; i++ {
			for _, expRes := range test.itrResults {
				kvp, buff, done := itr.nextForDestructiveMerge()

				kval, err := kvp.Key.Value(ctx)
				assert.NoError(t, err)

				if !kval.Equals(types.Uint(expRes.keyVal)) {
					t.Error("unexpected result")
				}

				if (buff != nil) != expRes.exhaustedBuf {
					t.Error("unexpected buffer result")
				}

				if done != expRes.done {
					t.Error("unexpected is done value.")
				}
			}

			itr.Reset()
		}
	}
}
