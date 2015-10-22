package types

import (
	"github.com/attic-labs/noms/ref"
)

type Ref struct {
	R ref.Ref
}

func (r Ref) Equals(other Value) bool {
	if other, ok := other.(Ref); ok {
		return r.Ref() == other.Ref()
	}
	return false
}

func (r Ref) Ref() ref.Ref {
	return r.R
}

func (r Ref) Chunks() []Future {
	return nil
}

var refTypeRef = MakeCompoundTypeRef("", RefKind, MakePrimitiveTypeRef(ValueKind))

func (r Ref) TypeRef() TypeRef {
	return refTypeRef
}

func init() {
	RegisterFromValFunction(refTypeRef, func(v Value) Value {
		return v.(Ref)
	})
}
