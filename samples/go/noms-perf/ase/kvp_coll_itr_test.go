package ase

import (
	"github.com/attic-labs/noms/go/types"
	"testing"
)

func TestKVPCollItr(t *testing.T) {
	slice1 := KVPSlice{{types.Uint(1), types.NullValue}, {types.Uint(2), types.NullValue}}
	slice2 := KVPSlice{{types.Uint(3), types.NullValue}, {types.Uint(4), types.NullValue}}
	slice3 := KVPSlice{{types.Uint(5), types.NullValue}, {}}

	type itrRes struct {
		keyVal       uint
		exhaustedBuf bool
		done         bool
	}
	tests := []struct {
		buffSize   int
		totalSize  int64
		slices     []KVPSlice
		itrResults []itrRes
	}{
		{
			2,
			5,
			[]KVPSlice{slice1, slice2, slice3[:1]},
			[]itrRes{
				{1, false, false},
				{2, true, false},
				{3, false, false},
				{4, true, false},
				{5, true, true},
			},
		},
	}

	for _, test := range tests {
		coll := &KVPCollection{test.buffSize, len(test.slices), test.totalSize, test.slices}
		itr := NewItr(coll)

		for i := 0; i < 2; i++ {
			for _, expRes := range test.itrResults {
				kvp, buff, done := itr.nextForDestructiveMerge()

				if !kvp.Key.Equals(types.Uint(expRes.keyVal)) {
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
