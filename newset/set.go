package newset

import (
	"github.com/attic-labs/noms/ref"
)

type Set interface {
	first() ref.Ref
	Len() uint64
	Has(r ref.Ref) bool
	Ref() ref.Ref
	fmt(indent int) string
}
