package types

import (
	"github.com/attic-labs/noms/ref"
)

// Reffer is a function that can compute a ref from a value. The Value package requires this for things like Ref(), Equals(), set membership, etc. Some other package must set this function during init.
var Reffer func(v Value) ref.Ref
