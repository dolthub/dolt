package types

import (
	"testing"
)

func TestAddKVP(t *testing.T) {
	builder := NewKVPCollBuilder(2)
	builder.AddKVP(KVP{Uint(0), NullValue})
	builder.AddKVP(KVP{Uint(1), NullValue})
	builder.AddKVP(KVP{Uint(2), NullValue})

	coll := builder.Build()
	itr := coll.Iterator()

	for i := int64(0); i < coll.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestMoveRemaining(t *testing.T) {
	sl1 := KVPSlice{{Uint(0), NullValue}, {Uint(1), NullValue}}
	sl2 := KVPSlice{{Uint(2), NullValue}, {}}
	coll := &KVPCollection{
		2,
		2,
		3,
		[]KVPSlice{sl1, sl2[:1]},
	}

	builder := NewKVPCollBuilder(2)
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}

func TestAddKVPAndMoveRemaining(t *testing.T) {
	sl := KVPSlice{{Uint(1), NullValue}, {Uint(2), NullValue}}
	coll := NewKVPCollection(sl)

	builder := NewKVPCollBuilder(2)
	builder.AddKVP(KVP{Uint(0), NullValue})
	builder.MoveRemaining(coll.Iterator())

	result := builder.Build()
	itr := result.Iterator()

	for i := int64(0); i < result.Size(); i++ {
		kvp := itr.Next()

		if uint(kvp.Key.(Uint)) != uint(i) {
			t.Error("enexpected value")
		}
	}
}
