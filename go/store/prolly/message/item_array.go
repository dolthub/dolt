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

// ItemArray is an array of Items
type ItemArray struct {
	Items []byte
	// Offs is an array of uint16 encoded offsets into |Items|.
	// the first offset is 0, the last offset if len(Items).
	Offs []byte
}

// GetItem returns the ith item in |arr|.
func (arr ItemArray) GetItem(i int) []byte {
	pos := i * 2
	start := val.ReadUint16(arr.Offs[pos : pos+2])
	stop := val.ReadUint16(arr.Offs[pos+2 : pos+4])
	return arr.Items[start:stop]
}

// Len returns the number of items in |arr|.
func (arr ItemArray) Len() int {
	return len(arr.Offs)/2 - 1
}
