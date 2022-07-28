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

type ItemArray struct {
	Buf  []byte
	Offs []byte
}

// GetSlice returns the ith slice of |sb.Items|.
func (sb ItemArray) GetSlice(i int) []byte {
	start := uint16(0)
	if i > 0 {
		pos := (i - 1) * 2
		start = val.ReadUint16(sb.Offs[pos : pos+2])
	}

	stop := uint16(len(sb.Buf))
	if i*2 < len(sb.Offs) {
		pos := i * 2
		stop = val.ReadUint16(sb.Offs[pos : pos+2])
	}

	return sb.Buf[start:stop]
}

func (sb ItemArray) Len() int {
	// offsets stored as uint16s with first offset omitted
	return len(sb.Offs)/2 + 1
}
