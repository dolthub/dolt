package edits

import (
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"testing"
)

func TestAddKVP(t *testing.T) {
	builder := NewKVPCollBuilder(2)
	builder.AddKVP(types.KVP{types.Uint(0), types.NullValue})
	builder.AddKVP(types.KVP{types.Uint(1), types.NullValue})
	builder.AddKVP(types.KVP{types.Uint(2), types.NullValue})

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
	sl1 := types.KVPSlice{{types.Uint(0), types.NullValue}, {types.Uint(1), types.NullValue}}
	sl2 := types.KVPSlice{{types.Uint(2), types.NullValue}, {}}
	coll := &KVPCollection{
		2,
		2,
		3,
		[]types.KVPSlice{sl1, sl2[:1]},
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
	sl := types.KVPSlice{{types.Uint(1), types.NullValue}, {types.Uint(2), types.NullValue}}
	coll := NewKVPCollection(sl)

	builder := NewKVPCollBuilder(2)
	builder.AddKVP(types.KVP{types.Uint(0), types.NullValue})
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
