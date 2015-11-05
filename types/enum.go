package types

import "github.com/attic-labs/noms/ref"

type Enum struct {
	v uint32
	t TypeRef
}

func newEnum(v uint32, t TypeRef) Enum {
	return Enum{v, t}
}

func (e Enum) Equals(other Value) bool {
	return other != nil && e.t.Equals(other.TypeRef()) && e.Ref() == other.Ref()
}

func (e Enum) Ref() ref.Ref {
	throwaway := ref.Ref{}
	return EnsureRef(&throwaway, e)
}

func (e Enum) Chunks() []ref.Ref {
	return nil
}

func (e Enum) ChildValues() []Value {
	return nil
}

func (e Enum) TypeRef() TypeRef {
	return e.t
}
