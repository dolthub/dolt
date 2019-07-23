// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
