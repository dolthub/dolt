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

package row

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestTupleValsLess(t *testing.T) {
	tests := []struct {
		name       string
		tags       []uint64
		lesserTVs  TaggedValues
		greaterTVs TaggedValues
		areEqual   bool
	}{
		{
			name:       "equal vals",
			tags:       []uint64{0},
			lesserTVs:  map[uint64]types.Value{0: types.String("a")},
			greaterTVs: map[uint64]types.Value{0: types.String("a")},
			areEqual:   true,
		},
		{
			name:       "equal vals with null",
			tags:       []uint64{0, 1},
			lesserTVs:  map[uint64]types.Value{0: types.String("a")},
			greaterTVs: map[uint64]types.Value{0: types.String("a")},
			areEqual:   true,
		},
		{
			name:       "null value after regular val",
			tags:       []uint64{0},
			lesserTVs:  map[uint64]types.Value{0: types.String("a")},
			greaterTVs: map[uint64]types.Value{0: types.NullValue},
			areEqual:   false,
		},
		{
			name:       "null value after regular val",
			tags:       []uint64{0},
			lesserTVs:  map[uint64]types.Value{0: types.String("a")},
			greaterTVs: map[uint64]types.Value{0: types.NullValue},
			areEqual:   false,
		},
		{
			name:       "null and null value equal",
			tags:       []uint64{0},
			lesserTVs:  map[uint64]types.Value{0: types.NullValue},
			greaterTVs: map[uint64]types.Value{},
			areEqual:   true,
		},
		{
			name:       "simple string",
			tags:       []uint64{0},
			lesserTVs:  map[uint64]types.Value{0: types.String("a")},
			greaterTVs: map[uint64]types.Value{0: types.String("b")},
			areEqual:   false,
		},
		{
			name:       "no tag overlap",
			tags:       []uint64{0, 1},
			lesserTVs:  map[uint64]types.Value{0: types.String("a")},
			greaterTVs: map[uint64]types.Value{1: types.String("a")},
			areEqual:   false,
		},
		{
			name:       "equal for supplied tags",
			tags:       []uint64{0},
			lesserTVs:  map[uint64]types.Value{0: types.String("a"), 1: types.Int(1)},
			greaterTVs: map[uint64]types.Value{0: types.String("a"), 1: types.Int(-1)},
			areEqual:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			lesserTplVals := test.lesserTVs.nomsTupleForTags(types.Format_7_18, test.tags, true)
			greaterTplVals := test.greaterTVs.nomsTupleForTags(types.Format_7_18, test.tags, true)

			lessLTGreater, err := lesserTplVals.Less(types.Format_7_18, greaterTplVals)
			assert.NoError(t, err)
			greaterLTLess, err := greaterTplVals.Less(types.Format_7_18, lesserTplVals)
			assert.NoError(t, err)

			assert.True(t, test.areEqual && !lessLTGreater || !test.areEqual && lessLTGreater)
			assert.True(t, !greaterLTLess)

			lesserTpl, err := lesserTplVals.Value(ctx)
			assert.NoError(t, err)
			greaterTpl, err := greaterTplVals.Value(ctx)
			assert.NoError(t, err)

			lesserLess, err := lesserTpl.Less(types.Format_7_18, greaterTpl)
			assert.NoError(t, err)
			greaterLess, err := greaterTpl.Less(types.Format_7_18, lesserTpl)
			assert.NoError(t, err)

			// needs to match with the types.Tuple Less implementation.
			assert.True(t, lessLTGreater == lesserLess)
			assert.True(t, greaterLTLess == greaterLess)
		})
	}
}

func mustTuple(tpl types.Tuple, err error) types.Tuple {
	if err != nil {
		panic(err)
	}

	return tpl
}

func TestTaggedTuple_NomsTupleForTags(t *testing.T) {
	ctx := context.Background()

	tt := TaggedValues{
		0: types.String("0"),
		1: types.String("1"),
		2: types.String("2")}

	tests := []struct {
		tags        []uint64
		encodeNulls bool
		want        types.Tuple
	}{
		{[]uint64{}, true, mustTuple(types.NewTuple(types.Format_7_18))},
		{[]uint64{1}, true, mustTuple(types.NewTuple(types.Format_7_18, types.Uint(1), types.String("1")))},
		{[]uint64{0, 1, 2}, true, mustTuple(types.NewTuple(types.Format_7_18, types.Uint(0), types.String("0"), types.Uint(1), types.String("1"), types.Uint(2), types.String("2")))},
		{[]uint64{2, 1, 0}, true, mustTuple(types.NewTuple(types.Format_7_18, types.Uint(2), types.String("2"), types.Uint(1), types.String("1"), types.Uint(0), types.String("0")))},
		{[]uint64{1, 3}, true, mustTuple(types.NewTuple(types.Format_7_18, types.Uint(1), types.String("1"), types.Uint(3), types.NullValue))},
		{[]uint64{1, 3}, false, mustTuple(types.NewTuple(types.Format_7_18, types.Uint(1), types.String("1")))},
		//{[]uint64{0, 1, 2}, types.NewTuple(types.Uint(0), types.String("0"), )},
		//{map[uint64]types.Value{}, []uint64{}, types.NewTuple()},
		//{map[uint64]types.Value{}, []uint64{}, types.NewTuple()},
	}
	for _, test := range tests {
		if got, err := tt.nomsTupleForTags(types.Format_7_18, test.tags, test.encodeNulls).Value(ctx); err != nil {
			t.Error(err)
		} else if !reflect.DeepEqual(got, test.want) {
			gotStr, err := types.EncodedValue(ctx, got)

			if err != nil {
				t.Error(err)
			}

			wantStr, err := types.EncodedValue(ctx, test.want)

			if err != nil {
				t.Error(err)
			}

			t.Errorf("TaggedValues.nomsTupleForTags() = %v, want %v", gotStr, wantStr)
		}
	}
}

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

	assert.NoError(t, err)

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

func TestTaggedTuple_Set(t *testing.T) {
	tests := []struct {
		tag  uint64
		val  types.Value
		want TaggedValues
	}{
		{1, types.String("one"), TaggedValues{1: types.String("one"), 2: types.String("2"), 3: types.String("3")}},
		{0, types.String("0"), TaggedValues{0: types.String("0"), 1: types.String("1"), 2: types.String("2"), 3: types.String("3")}},
	}
	for _, test := range tests {
		tt := TaggedValues{
			1: types.String("1"),
			2: types.String("2"),
			3: types.String("3")}

		if got := tt.Set(test.tag, test.val); !reflect.DeepEqual(got, test.want) {
			t.Errorf("TaggedValues.Set() = %v, want %v", got, test.want)
		}
	}
}

func TestParseTaggedTuple(t *testing.T) {
	tests := []struct {
		tpl  types.Tuple
		want TaggedValues
	}{
		{
			mustTuple(types.NewTuple(types.Format_7_18)),
			TaggedValues{},
		},
		{
			mustTuple(types.NewTuple(types.Format_7_18, types.Uint(0), types.String("0"))),
			TaggedValues{0: types.String("0")},
		},
		{
			mustTuple(types.NewTuple(types.Format_7_18, types.Uint(0), types.String("0"), types.Uint(5), types.Uint(5), types.Uint(60), types.Int(60))),
			TaggedValues{0: types.String("0"), 5: types.Uint(5), 60: types.Int(60)},
		},
	}
	for _, test := range tests {
		if got, err := ParseTaggedValues(test.tpl); err != nil {
			t.Error(err)
		} else if !reflect.DeepEqual(got, test.want) {
			t.Errorf("ParseTaggedValues() = %v, want %v", got, test.want)
		}
	}
}
