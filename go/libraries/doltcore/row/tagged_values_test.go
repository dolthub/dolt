package row

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/attic-labs/noms/go/types"
)

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
		{[]uint64{}, true, types.NewTuple()},
		{[]uint64{1}, true, types.NewTuple(types.Uint(1), types.String("1"))},
		{[]uint64{0, 1, 2}, true, types.NewTuple(types.Uint(0), types.String("0"), types.Uint(1), types.String("1"), types.Uint(2), types.String("2"))},
		{[]uint64{2, 1, 0}, true, types.NewTuple(types.Uint(2), types.String("2"), types.Uint(1), types.String("1"), types.Uint(0), types.String("0"))},
		{[]uint64{1, 3}, true, types.NewTuple(types.Uint(1), types.String("1"), types.Uint(3), types.NullValue)},
		{[]uint64{1, 3}, false, types.NewTuple(types.Uint(1), types.String("1"))},
		//{[]uint64{0, 1, 2}, types.NewTuple(types.Uint(0), types.String("0"), )},
		//{map[uint64]types.Value{}, []uint64{}, types.NewTuple()},
		//{map[uint64]types.Value{}, []uint64{}, types.NewTuple()},
	}
	for _, test := range tests {
		if got := tt.NomsTupleForTags(test.tags, test.encodeNulls).Value(ctx); !reflect.DeepEqual(got, test.want) {
			t.Errorf("TaggedValues.NomsTupleForTags() = %v, want %v", types.EncodedValue(ctx, got), types.EncodedValue(ctx, test.want))
		}
	}
}

func TestTaggedTuple_Iter(t *testing.T) {
	tt := TaggedValues{
		1: types.String("1"),
		2: types.String("2"),
		3: types.String("3")}

	var sum uint64
	tt.Iter(func(tag uint64, val types.Value) (stop bool) {
		sum += tag
		tagStr := strconv.FormatUint(tag, 10)
		if !types.String(tagStr).Equals(val) {
			t.Errorf("Unexpected value for tag %d: %s", sum, string(val.(types.String)))
		}
		return false
	})

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
			t.Errorf("TaggedValues.Get() = %s, want %s", types.EncodedValue(context.Background(), got), types.EncodedValue(context.Background(), test.want))
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
			types.NewTuple(),
			TaggedValues{},
		},
		{
			types.NewTuple(types.Uint(0), types.String("0")),
			TaggedValues{0: types.String("0")},
		},
		{
			types.NewTuple(types.Uint(0), types.String("0"), types.Uint(5), types.Uint(5), types.Uint(60), types.Int(60)),
			TaggedValues{0: types.String("0"), 5: types.Uint(5), 60: types.Int(60)},
		},
	}
	for _, test := range tests {
		if got := ParseTaggedValues(test.tpl); !reflect.DeepEqual(got, test.want) {
			t.Errorf("ParseTaggedValues() = %v, want %v", got, test.want)
		}
	}
}
