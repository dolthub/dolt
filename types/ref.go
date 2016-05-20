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

func NewRef(v Value) Ref {
	return Ref{v.Ref(), maxChunkHeight(v) + 1, MakeRefType(v.Type()), &ref.Ref{}}
}

// Constructs a Ref directly from struct properties. This should not be used outside decoding and testing within the types package.
func constructRef(t *Type, target ref.Ref, height uint64) Ref {
	d.Chk.Equal(RefKind, t.Kind(), "Invalid type. Expected: RefKind, found: %s", t.Describe())
	d.Chk.NotEqual(ValueType, t.Desc.(CompoundDesc).ElemTypes[0])
	return Ref{target, height, t, &ref.Ref{}}
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

func (r Ref) TargetRef() ref.Ref {
	return r.target
}

func (r Ref) Height() uint64 {
	return r.height
}

func (r Ref) TargetValue(vr ValueReader) Value {
	return vr.ReadValue(r.target)
}

// Value interface
func (r Ref) Equals(other Value) bool {
	return other != nil && r.t.Equals(other.Type()) && r.Ref() == other.Ref()
}

func (r Ref) Less(other Value) bool {
	return valueLess(r, other)
}

func (r Ref) Ref() ref.Ref {
	return EnsureRef(r.ref, r)
}

func (r Ref) ChildValues() []Value {
	return nil
}

func (r Ref) Chunks() (chunks []Ref) {
	return append(chunks, r)
}

func (r Ref) Type() *Type {
	return r.t
}
