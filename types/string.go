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

func (fs String) Blob() Blob {
	return NewBlob(bytes.NewBufferString(fs.s))
}

func (fs String) String() string {
	return fs.s
}

func (fs String) Ref() ref.Ref {
	return ensureRef(fs.ref, fs)
}

func (fs String) Equals(other Value) bool {
	if other == nil {
		return false
	} else {
		return fs.Ref() == other.Ref()
	}
}

func (fs String) Chunks() []Future {
	return nil
}

func StringFromVal(v Value) String {
	return v.(String)
}
