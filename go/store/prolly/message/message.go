// Copyright 2022 Dolthub, Inc.
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
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

type Message []byte

func GetKeys(msg Message) val.SlicedBuffer {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapKeys(msg)
	case serial.AddressMapFileID:
		return getAddressMapKeys(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetValues(msg Message) val.SlicedBuffer {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapValues(msg)
	case serial.AddressMapFileID:
		return getAddressMapValues(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func WalkAddresses(ctx context.Context, msg Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return walkProllyMapAddresses(ctx, msg, cb)
	case serial.AddressMapFileID:
		return walkAddressMapAddresses(ctx, msg, cb)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetCount(msg Message) uint16 {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapCount(msg)
	case serial.AddressMapFileID:
		return getAddressMapCount(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetTreeLevel(msg Message) int {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapTreeLevel(msg)
	case serial.AddressMapFileID:
		return getAddressMapTreeLevel(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetTreeCount(msg Message) int {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapTreeCount(msg)
	case serial.AddressMapFileID:
		return getAddressMapTreeCount(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetSubtrees(msg Message) []uint64 {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapSubtrees(msg)
	case serial.AddressMapFileID:
		return getAddressMapSubtrees(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func assertTrue(b bool) {
	if !b {
		panic("assertion failed")
	}
}

func assertFalse(b bool) {
	if b {
		panic("assertion failed")
	}
}
