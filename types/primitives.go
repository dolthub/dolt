package types

import (
	"github.com/attic-labs/noms/ref"
)

type Bool bool

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

func (v Bool) Chunks() []Ref {
	return nil
}

func (v Bool) ChildValues() []Value {
	return nil
}

func (v Bool) ToPrimitive() interface{} {
	return bool(v)
}

func (v Bool) Type() *Type {
	return BoolType
}

type Number float64

func (v Number) Equals(other Value) bool {
	return v == other
}

func (v Number) Ref() ref.Ref {
	return getRef(v)
}

func (v Number) Chunks() []Ref {
	return nil
}

func (v Number) ChildValues() []Value {
	return nil
}

func (v Number) ToPrimitive() interface{} {
	return float64(v)
}

func (v Number) Type() *Type {
	return NumberType
}

func (v Number) Less(other Value) bool {
	if v2, ok := other.(Number); ok {
		return float64(v) < float64(v2)
	}
	return NumberKind < other.Type().Kind()
}
