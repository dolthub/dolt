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
				{types.Uint(5), types.NullValue},
				{types.Uint(1), types.NullValue},
				{types.Uint(4), types.NullValue},
				{types.Uint(3), types.NullValue},
			},
			types.KVPSlice{
				{types.Uint(1), types.NullValue},
				{types.Uint(3), types.NullValue},
				{types.Uint(4), types.NullValue},
				{types.Uint(5), types.NullValue},
			},
		},
	}

	for _, test := range tests {
		IsInOrder(NewItr(types.Format_7_18, NewKVPCollection(types.Format_7_18, test.kvps)))
		sort.Stable(types.KVPSort{test.kvps, types.Format_7_18})

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
