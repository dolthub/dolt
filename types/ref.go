package types

import (
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type Ref struct {
	target ref.Ref
	height uint64
	t      *Type
	ref    *ref.Ref
}

func NewTypedRef(t *Type, target ref.Ref, height uint64) Ref {
	d.Chk.Equal(RefKind, t.Kind(), "Invalid type. Expected: RefKind, found: %s", t.Describe())
	return Ref{target, height, t, &ref.Ref{}}
}

func NewTypedRefFromValue(v Value) Ref {
	return NewTypedRef(MakeRefType(v.Type()), v.Ref(), maxChunkHeight(v)+1)
}

func maxChunkHeight(v Value) (max uint64) {
	if chunks := v.Chunks(); chunks != nil {
		for _, r := range chunks {
			if height := r.Height(); height > max {
				max = height
			}
		}
	}
	return
}

func (r Ref) Equals(other Value) bool {
	return other != nil && r.t.Equals(other.Type()) && r.Ref() == other.Ref()
}

func (r Ref) Ref() ref.Ref {
	return EnsureRef(r.ref, r)
}

func (r Ref) Chunks() (chunks []Ref) {
	return append(chunks, r)
}

func (r Ref) ChildValues() []Value {
	return nil
}

func (r Ref) TargetRef() ref.Ref {
	return r.target
}

func (r Ref) Height() uint64 {
	return r.height
}

func (r Ref) Type() *Type {
	return r.t
}

func (r Ref) Less(other OrderedValue) bool {
	return r.target.Less(other.(Ref).target)
}

func (r Ref) TargetValue(vr ValueReader) Value {
	return vr.ReadValue(r.target)
}
