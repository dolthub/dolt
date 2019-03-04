package valutil

import "github.com/attic-labs/noms/go/types"

func NilSafeEqCheck(v1, v2 types.Value) bool {
	if types.IsNull(v1) {
		return types.IsNull(v2)
	} else {
		return v1.Equals(v2)
	}
}
