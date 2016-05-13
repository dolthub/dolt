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

// Value interface
func (s String) Equals(other Value) bool {
	if other, ok := other.(String); ok {
		return s.s == other.s
	}
	return false
}

func (s String) Less(other Value) bool {
	if s2, ok := other.(String); ok {
		return s.s < s2.s
	}
	return StringKind < other.Type().Kind()
}

func (fs String) Ref() ref.Ref {
	return EnsureRef(fs.ref, fs)
}

func (fs String) ChildValues() []Value {
	return nil
}

func (fs String) Chunks() []Ref {
	return nil
}

func (fs String) Type() *Type {
	return StringType
}
