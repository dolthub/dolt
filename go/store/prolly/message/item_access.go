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

import (
	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/val"
)

// ItemAccess accesses items in a serial.Message.
type ItemAccess struct {
	// bufStart is the offset to the start of the
	// Item buffer within a serial.Message.
	// bufLen is the length of the Item buffer.
	bufStart, bufLen uint16

	// offStart, if nonzero, is the offset to the
	// start of the uin16 offset buffer within a
	// serial.Message. A zero value for offStart
	// indicates an empty offset buffer.
	// bufLen is the length of the Item buffer.
	offStart, offLen uint16

	// If the serial.Message does not contain an
	// offset buffer (offStart is zero), then
	// Items have a fixed width equal to itemWidth.
	itemWidth uint16
}

// GetItem returns the ith Item from the buffer.
func (acc ItemAccess) GetItem(i int, msg serial.Message) []byte {
	buf := msg[acc.bufStart : acc.bufStart+acc.bufLen]
	off := msg[acc.offStart : acc.offStart+acc.offLen]
	if acc.offStart != 0 {
		stop := val.ReadUint16(off[(i*2)+2 : (i*2)+4])
		start := val.ReadUint16(off[(i * 2) : (i*2)+2])
		return buf[start:stop]
	} else {
		stop := int(acc.itemWidth) * (i + 1)
		start := int(acc.itemWidth) * i
		return buf[start:stop]
	}
}
