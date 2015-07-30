package types

import (
	"github.com/attic-labs/noms/ref"
)

// Value is implemented by every noms value
type Value interface {
	Equals(other Value) bool
	Ref() ref.Ref
	Chunks() []Future
	Release()
}
