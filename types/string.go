package types

import (
	"bytes"

	"github.com/attic-labs/noms/ref"
)

type String struct {
	s   string
	ref *ref.Ref
}

func NewString(s string) String {
	return String{s, &ref.Ref{}}
}

func (fs String) Blob() (Blob, error) {
	return NewBlob(bytes.NewBufferString(fs.s))
}

func (fs String) String() string {
	return fs.s
}

func (fs String) Ref() ref.Ref {
	return EnsureRef(fs.ref, fs)
}

func (s String) Equals(other Value) bool {
	return other != nil && s.Ref() == other.Ref()
}

func (fs String) Chunks() []ref.Ref {
	return nil
}

var typeRefForString = MakePrimitiveTypeRef(StringKind)

func (fs String) TypeRef() TypeRef {
	return typeRefForString
}

func StringFromVal(v Value) String {
	return v.(String)
}
