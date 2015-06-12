package types

import (
	"github.com/attic-labs/noms/ref"
)

// Stupid inefficient temporary implementation of the String interface.
type flatString struct {
	s  string
	cr *cachedRef
}

func (fs flatString) Blob() Blob {
	return NewBlob([]byte(fs.s))
}

func (fs flatString) String() string {
	return fs.s
}

func (fs flatString) Ref() ref.Ref {
	return fs.cr.Ref(fs)
}

func (fs flatString) Equals(other Value) bool {
	if other, ok := other.(String); ok {
		return fs.String() == other.String()
	} else {
		return false
	}
}
