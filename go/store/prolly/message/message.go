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
	"math"

	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	maxChunkSz = math.MaxUint16
	addrSize   = hash.ByteLen
	uint16Size = 2
)

type Serializer interface {
	Serialize(keys, values [][]byte, subtrees []uint64, level int) serial.Message
}

func UnpackFields(msg serial.Message) (keys, values ItemAccess, level, count uint16, err error) {
	id := serial.GetFileID(msg)

	if id == serial.ProllyTreeNodeFileID {
		return getProllyMapKeysAndValues(msg)
	}
	if id == serial.AddressMapFileID {
		keys, err = getAddressMapKeys(msg)
		if err != nil {
			return
		}
		values, err = getAddressMapValues(msg)
		if err != nil {
			return
		}
		level, err = getAddressMapTreeLevel(msg)
		if err != nil {
			return
		}
		count, err = getAddressMapCount(msg)
		return
	}
	if id == serial.MergeArtifactsFileID {
		return getArtifactMapKeysAndValues(msg)
	}
	if id == serial.CommitClosureFileID {
		keys, err = getCommitClosureKeys(msg)
		if err != nil {
			return
		}
		values, err = getCommitClosureValues(msg)
		if err != nil {
			return
		}
		level, err = getCommitClosureTreeLevel(msg)
		if err != nil {
			return
		}
		count, err = getCommitClosureCount(msg)
		return
	}
	if id == serial.BlobFileID {
		keys, err = getBlobKeys(msg)
		if err != nil {
			return
		}
		values, err = getBlobValues(msg)
		if err != nil {
			return
		}
		level, err = getBlobTreeLevel(msg)
		if err != nil {
			return
		}
		count, err = getBlobCount(msg)
		return
	}
	panic(fmt.Sprintf("unknown message id %s", id))
}

func WalkAddresses(ctx context.Context, msg serial.Message, cb func(ctx context.Context, addr hash.Hash) error) error {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return walkProllyMapAddresses(ctx, msg, cb)
	case serial.AddressMapFileID:
		return walkAddressMapAddresses(ctx, msg, cb)
	case serial.MergeArtifactsFileID:
		return walkMergeArtifactAddresses(ctx, msg, cb)
	case serial.CommitClosureFileID:
		return walkCommitClosureAddresses(ctx, msg, cb)
	case serial.BlobFileID:
		return walkBlobAddresses(ctx, msg, cb)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetTreeCount(msg serial.Message) (int, error) {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapTreeCount(msg)
	case serial.AddressMapFileID:
		return getAddressMapTreeCount(msg)
	case serial.MergeArtifactsFileID:
		return getMergeArtifactTreeCount(msg)
	case serial.CommitClosureFileID:
		return getCommitClosureTreeCount(msg)
	case serial.BlobFileID:
		return getBlobTreeCount(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func GetSubtrees(msg serial.Message) ([]uint64, error) {
	id := serial.GetFileID(msg)
	switch id {
	case serial.ProllyTreeNodeFileID:
		return getProllyMapSubtrees(msg)
	case serial.AddressMapFileID:
		return getAddressMapSubtrees(msg)
	case serial.MergeArtifactsFileID:
		return getMergeArtifactSubtrees(msg)
	case serial.CommitClosureFileID:
		return getCommitClosureSubtrees(msg)
	case serial.BlobFileID:
		return getBlobSubtrees(msg)
	default:
		panic(fmt.Sprintf("unknown message id %s", id))
	}
}

func lookupVectorOffset(vo fb.VOffsetT, tab fb.Table) uint16 {
	off := fb.UOffsetT(tab.Offset(vo)) + tab.Pos
	off += fb.GetUOffsetT(tab.Bytes[off:])
	// data starts after metadata containing the vector length
	return uint16(off + fb.UOffsetT(fb.SizeUOffsetT))
}

func assertTrue(b bool, msg string) {
	if !b {
		panic("assertion failed: " + msg)
	}
}

func assertFalse(b bool, msg string) {
	if b {
		panic("assertion failed: " + msg)
	}
}
