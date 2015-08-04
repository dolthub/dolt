package types

import (
	"github.com/attic-labs/noms/ref"
)

type Ref struct {
	R ref.Ref
}

func (r Ref) Equals(other Value) bool {
	return r.R == other.Ref()
}

func (r Ref) Ref() ref.Ref {
	return r.R
}

func (r Ref) Chunks() []Future {
	return nil
}
