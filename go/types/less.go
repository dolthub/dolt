// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

func valueLess(v1, v2 Value) bool {
	switch v2.Kind() {
	case BoolKind, NumberKind, StringKind:
		return false
	default:
		return v1.Hash().Less(v2.Hash())
	}
}
