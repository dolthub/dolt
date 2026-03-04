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

package row

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestTaggedTuple_Iter(t *testing.T) {
	tt := TaggedValues{
		1: types.String("1"),
		2: types.String("2"),
		3: types.String("3")}

	var sum uint64
	_, err := tt.Iter(func(tag uint64, val types.Value) (stop bool, err error) {
		sum += tag
		tagStr := strconv.FormatUint(tag, 10)
		if !types.String(tagStr).Equals(val) {
			t.Errorf("Unexpected value for tag %d: %s", sum, string(val.(types.String)))
		}
		return false, nil
	})

	require.NoError(t, err)

	if sum != 6 {
		t.Error("Did not iterate all tags.")
	}
}

func TestTaggedTuple_Get(t *testing.T) {
	tt := TaggedValues{
		1: types.String("1"),
		2: types.String("2"),
		3: types.String("3")}

	tests := []struct {
		tag   uint64
		want  types.Value
		found bool
	}{
		{1, types.String("1"), true},
		{4, nil, false},
	}
	for _, test := range tests {
		got, ok := tt.Get(test.tag)
		if ok != test.found {
			t.Errorf("expected to be found: %v, found: %v", ok, test.found)
		} else if !reflect.DeepEqual(got, test.want) {
			gotStr, err := types.EncodedValue(context.Background(), got)

			if err != nil {
				t.Error(err)
			}

			wantStr, err := types.EncodedValue(context.Background(), test.want)

			if err != nil {
				t.Error(err)
			}

			t.Errorf("TaggedValues.Get() = %s, want %s", gotStr, wantStr)
		}
	}
}
