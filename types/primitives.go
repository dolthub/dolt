package types

import (
	"github.com/attic-labs/noms/ref"
)

type Bool bool

func (p Bool) Equals(other Value) bool {
	return p == other
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

var typeForBool = BoolType

func (v Bool) Type() *Type {
	return typeForBool
}

type Number float64

func (p Number) Equals(other Value) bool {
	return p == other
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

var typeForNumber = NumberType

func (v Number) Type() *Type {
	return typeForNumber
}

func (v Number) Less(other OrderedValue) bool {
	return v < other.(Number)
}
