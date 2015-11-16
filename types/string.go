package types

import "github.com/attic-labs/noms/ref"

type String struct {
	s   string
	ref *ref.Ref
}

func NewString(s string) String {
	return String{s, &ref.Ref{}}
}

func (fs String) String() string {
	return fs.s
}

func (fs String) Ref() ref.Ref {
	return EnsureRef(fs.ref, fs)
}

func (s String) Equals(other Value) bool {
	if other, ok := other.(String); ok {
		return s.s == other.s
	}
	return false
}

func (s String) Less(other OrderedValue) bool {
	return s.s < other.(String).s
}

func (fs String) Chunks() []ref.Ref {
	return nil
}

func (fs String) ChildValues() []Value {
	return nil
}

var typeForString = MakePrimitiveType(StringKind)

func (fs String) Type() Type {
	return typeForString
}
