package types

import (
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

type Ref struct {
	target ref.Ref
	ref    *ref.Ref
}

func NewRef(target ref.Ref) Ref {
	return Ref{target, &ref.Ref{}}
}

func (r Ref) Equals(other Value) bool {
	if other, ok := other.(Ref); ok {
		return r.Ref() == other.Ref()
	}
	return false
}

func (r Ref) Ref() ref.Ref {
	return EnsureRef(r.ref, r)
}

func (r Ref) Chunks() []Future {
	return nil
}

func (r Ref) TargetRef() ref.Ref {
	return r.target
}

var refTypeRef = MakeCompoundTypeRef(RefKind, MakePrimitiveTypeRef(ValueKind))

func (r Ref) TypeRef() TypeRef {
	return refTypeRef
}

func init() {
	RegisterFromValFunction(refTypeRef, func(v Value) Value {
		return v.(Ref)
	})
}

func (r Ref) TargetValue(cs chunks.ChunkSource) Value {
	return ReadValue(r.target, cs)
}

func (r Ref) SetTargetValue(val Value, cs chunks.ChunkSink) Ref {
	return NewRef(WriteValue(val, cs))
}
