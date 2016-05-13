package types

import (
	"github.com/attic-labs/noms/ref"
)

type Bool bool

// Value interface
func (v Bool) Equals(other Value) bool {
	return v == other
}

func (v Bool) Less(other Value) bool {
	if v2, ok := other.(Bool); ok {
		return !bool(v) && bool(v2)
	}
	return true
}

func (v Bool) Ref() ref.Ref {
	return getRef(v)
}

func (v Bool) ChildValues() []Value {
	return nil
}

func (v Bool) Chunks() []Ref {
	return nil
}

func (v Bool) Type() *Type {
	return BoolType
}

// ValueWriter - primitive interface
func (v Bool) ToPrimitive() interface{} {
	return bool(v)
}
