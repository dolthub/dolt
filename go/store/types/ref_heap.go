// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

// HeightOrder returns true if a is 'higher than' b, generally if its ref-height is greater. If the two are of the same height, fall back to sorting by TargetHash.
func HeightOrder(a, b Ref) bool {
	if a.Height() == b.Height() {
		return a.TargetHash().Less(b.TargetHash())
	}
	// > because we want the larger heights to be at the start of the queue.
	return a.Height() > b.Height()

}

// RefSlice implements sort.Interface to order by target ref.
type RefSlice []Ref

func (s RefSlice) Len() int {
	return len(s)
}

func (s RefSlice) Less(i, j int) bool {
	return s[i].TargetHash().Less(s[j].TargetHash())
}

func (s RefSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
