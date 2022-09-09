// Copyright 2021 Dolthub, Inc.
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

package message

import "github.com/dolthub/dolt/go/store/val"

// ItemAccess is an array of Items
type ItemAccess struct {
	items []byte
	// Offs is an array of uint16 encoded offsets into |items|.
	// the first offset is 0, the last offset if len(Items).
	offs []byte
}

// GetItem returns the ith item in |arr|.
func (arr ItemAccess) GetItem(i int) []byte {
	pos := i * 2
	start := val.ReadUint16(arr.offs[pos : pos+2])
	stop := val.ReadUint16(arr.offs[pos+2 : pos+4])
	return arr.items[start:stop]
}

// Len returns the number of items in |arr|.
func (arr ItemAccess) Len() int {
	return len(arr.offs)/2 - 1
}
