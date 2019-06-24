package valutil

import (
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// NilSafeEqCheck compares two types.Value instances in a manner that handles nils as equal to types.NullValue
func NilSafeEqCheck(v1, v2 types.Value) bool {
	if types.IsNull(v1) {
		return types.IsNull(v2)
	} else {
		return v1.Equals(v2)
	}
}
