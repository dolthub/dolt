package types

import (
	"github.com/attic-labs/noms/ref"
)

// Value is implemented by every noms value
type Value interface {
	Equals(other Value) bool
	Ref() ref.Ref
	// Returns the immediate children of this value in the DAG, if any, not including Type().
	ChildValues() []Value
	Chunks() []RefBase
	Type() Type
}

type OrderedValue interface {
	Value
	Less(other OrderedValue) bool
}
