// Copyright 2026 Dolthub, Inc.
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

package creation

import (
	"github.com/dolthub/dolt/go/store/prolly/sort"
	"github.com/dolthub/dolt/go/store/types"
)

// spillSealer returns the sealer of the tuples that building an index of |vrw|'s data spills to disk, or nil to spill
// them as they are. Dolt Enterprise replaces it.
var spillSealer = func(vrw types.ValueReadWriter) (sort.SpillSealer, error) {
	return nil, nil
}
