package types

import "github.com/attic-labs/noms/ref"

type Resolver func(ref ref.Ref) (Value, error)
