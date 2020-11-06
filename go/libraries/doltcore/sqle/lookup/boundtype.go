// Copyright 2020 Dolthub, Inc.
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

package lookup

// BoundType is the bound of the Cut.
type BoundType int

const (
	// Open bounds represent exclusion.
	Open BoundType = iota
	// Closed bounds represent inclusion.
	Closed
)

// Inclusive returns whether the bound represents inclusion.
func (bt BoundType) Inclusive() bool {
	return bt == Closed
}
