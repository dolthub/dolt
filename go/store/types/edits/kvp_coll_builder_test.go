package edits

import (
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func TestAddKVP(t *testing.T) {
	builder := NewKVPCollBuilder(2)
	builder.AddKVP(types.KVP{Key: types.Uint(0), Val: types.NullValue})
	builder.AddKVP(types.KVP{Key: types.Uint(1), Val: types.NullValue})
	builder.AddKVP(types.KVP{Key: types.Uint(2), Val: types.NullValue})

	coll := builder.Build()
	itr := coll.Iterator()

	for i := int64(0); i < coll.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestMoveRemaining(t *testing.T) {
	sl1 := types.KVPSlice{{Key: types.Uint(0), Val: types.NullValue}, {Key: types.Uint(1), Val: types.NullValue}}
	sl2 := types.KVPSlice{{Key: types.Uint(2), Val: types.NullValue}, {}}
	coll := &KVPCollection{
		2,
		2,
		3,
		[]types.KVPSlice{sl1, sl2[:1]},
		types.Format_7_18,
	}

	builder := NewKVPCollBuilder(2)
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestAddKVPAndMoveRemaining(t *testing.T) {
	sl := types.KVPSlice{{Key: types.Uint(1), Val: types.NullValue}, {Key: types.Uint(2), Val: types.NullValue}}
	coll := NewKVPCollection(types.Format_7_18, sl)

	builder := NewKVPCollBuilder(2)
	builder.AddKVP(types.KVP{Key: types.Uint(0), Val: types.NullValue})
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(types.Uint)) != uint(i) {
			t.Error("unexpected value")
		}
	}
}
