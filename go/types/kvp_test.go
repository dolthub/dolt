package types

import (
	"context"
	"sort"
	"testing"
)

func TestKVPSliceSort(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		kvps      KVPSlice
		expSorted KVPSlice
	}{
		{
			KVPSlice{
				{Uint(5), NullValue},
				{Uint(1), NullValue},
				{Uint(4), NullValue},
				{Uint(3), NullValue},
			},
			KVPSlice{
				{Uint(1), NullValue},
				{Uint(3), NullValue},
				{Uint(4), NullValue},
				{Uint(5), NullValue},
			},
		},
	}

	for _, test := range tests {
		IsInOrder(NewItr(NewKVPCollection(test.kvps)))
		sort.Stable(test.kvps)

		if test.kvps.Len() != test.expSorted.Len() {
			t.Error("bad length")
		}

		for i := 0; i < test.kvps.Len(); i++ {
			if !test.kvps[i].Key.Value(ctx).Equals(test.expSorted[i].Key.Value(ctx)) {
				t.Error("value at", i, "does not match expected.")
			}
		}
	}
}
