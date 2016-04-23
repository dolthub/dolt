package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type Ref struct {
	target ref.Ref
	t      *Type
	ref    *ref.Ref
}

type RefBase interface {
	Value
	TargetRef() ref.Ref
}

func NewRef(target ref.Ref) Ref {
	return NewTypedRef(refType, target)
}

func NewTypedRef(t *Type, target ref.Ref) Ref {
	d.Chk.Equal(RefKind, t.Kind(), "Invalid type. Expected: RefKind, found: %s", t.Describe())
	return Ref{target, t, &ref.Ref{}}
}

func NewTypedRefFromValue(v Value) Ref {
	return NewTypedRef(MakeRefType(v.Type()), v.Ref())
}

func (r Ref) Equals(other Value) bool {
	return other != nil && r.t.Equals(other.Type()) && r.Ref() == other.Ref()
}

func (r Ref) Ref() ref.Ref {
	return EnsureRef(r.ref, r)
}

func (r Ref) Chunks() (chunks []RefBase) {
	return append(chunks, r)
}

func (r Ref) ChildValues() []Value {
	return nil
}

func (r Ref) TargetRef() ref.Ref {
	return r.target
}

var refType = MakeRefType(ValueType)

func (r Ref) Type() *Type {
	return r.t
}

func (r Ref) Less(other OrderedValue) bool {
	return r.target.Less(other.(Ref).target)
}

func (r Ref) TargetValue(vr ValueReader) Value {
	return vr.ReadValue(r.target)
}
