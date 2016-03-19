package types

import "github.com/attic-labs/noms/ref"

type Enum struct {
	v uint32
	t Type
}

func newEnum(v uint32, t Type) Enum {
	return Enum{v, t}
}

func (e Enum) Equals(other Value) bool {
	return other != nil && e.t.Equals(other.Type()) && e.Ref() == other.Ref()
}

func (e Enum) Ref() ref.Ref {
	throwaway := ref.Ref{}
	return EnsureRef(&throwaway, e)
}

func (e Enum) Chunks() []RefBase {
	return nil
}

func (e Enum) ChildValues() []Value {
	return nil
}

func (e Enum) Type() Type {
	return e.t
}
