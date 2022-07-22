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
	"encoding/binary"
	"fmt"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

const MessageTypesKind int = 27

const MessagePrefixSz = 4

type Message []byte

func FinishMessage(b *fb.Builder, off fb.UOffsetT, fileID []byte) Message {
	// We finish the buffer by prefixing it with:
	// 1) 1 byte NomsKind == SerialMessage.
	// 2) big endian 3 byte uint representing the size of the message, not
	// including the kind or size prefix bytes.
	//
	// This allows chunks we serialize here to be read by types binary
	// codec.
	//
	// All accessors in this package expect this prefix to be on the front
	// of the message bytes as well. See |MessagePrefixSz|.

	b.Prep(1, fb.SizeInt32+4+MessagePrefixSz)
	b.FinishWithFileIdentifier(off, fileID)

	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(b.Bytes)-int(b.Head())))
	if size[0] != 0 {
		panic("message is too large to be encoded")
	}

	bytes := b.Bytes[b.Head()-MessagePrefixSz:]
	bytes[0] = byte(MessageTypesKind)
	copy(bytes[1:], size[1:])
	return bytes
}

type Serializer interface {
	Serialize(keys, values [][]byte, subtrees []uint64, level int) Message
}

func GetKeysAndValues(msg Message) (keys, values val.SlicedBuffer, cnt uint16) {
	id := serial.GetFileID(msg[MessagePrefixSz:])

	if id == serial.ProllyTreeNodeFileID {
		return getProllyMapKeysAndValues(msg)
	}
	if id == serial.AddressMapFileID {
		keys = getAddressMapKeys(msg)
		values = getAddressMapValues(msg)
		cnt = getAddressMapCount(msg)
		return
	}
	if id == serial.CommitClosureFileID {
		keys = getCommitClosureKeys(msg)
		values = getCommitClosureValues(msg)
		cnt = getCommitClosureCount(msg)
		return
	}

	panic(fmt.Sprintf("unknown message id %s", id))
}

func WalkAddresses(ctx context.Context, msg Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	id := serial.GetFileID(msg[MessagePrefixSz:])
	switch id {
	case serial.ProllyTreeNodeFileID:
		return walkProllyMapAddresses(ctx, msg, cb)
	case serial.AddressMapFileID:
		return walkAddressMapAddresses(ctx, msg, cb)
	case serial.CommitClosureFileID:
		return walkCommitClosureAddresses(ctx, msg, cb)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetTreeLevel(msg Message) int {
	id := serial.GetFileID(msg[MessagePrefixSz:])
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapTreeLevel(msg)
	case serial.AddressMapFileID:
		return getAddressMapTreeLevel(msg)
	case serial.CommitClosureFileID:
		return getCommitClosureTreeLevel(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetTreeCount(msg Message) int {
	id := serial.GetFileID(msg[MessagePrefixSz:])
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapTreeCount(msg)
	case serial.AddressMapFileID:
		return getAddressMapTreeCount(msg)
	case serial.CommitClosureFileID:
		return getCommitClosureTreeCount(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetSubtrees(msg Message) []uint64 {
	id := serial.GetFileID(msg[MessagePrefixSz:])
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapSubtrees(msg)
	case serial.AddressMapFileID:
		return getAddressMapSubtrees(msg)
	case serial.CommitClosureFileID:
		return getCommitClosureSubtrees(msg)
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
