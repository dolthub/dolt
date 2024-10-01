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

package serial

import (
	"encoding/binary"
	"unsafe"

	fb "github.com/dolthub/flatbuffers/v23/go"
)

// KEEP THESE IN SYNC WITH .fbs FILES!

const StoreRootFileID = "STRT"
const TagFileID = "DTAG"
const WorkingSetFileID = "WRST"
const CommitFileID = "DCMT"
const RootValueFileID = "RTVL"
const TableFileID = "DTBL"
const ProllyTreeNodeFileID = "TUPM"
const AddressMapFileID = "ADRM"
const CommitClosureFileID = "CMCL"
const TableSchemaFileID = "DSCH"
const ForeignKeyCollectionFileID = "DFKC"
const MergeArtifactsFileID = "ARTM"
const BlobFileID = "BLOB"
const BranchControlFileID = "BRCL"
const StashListFileID = "SLST"
const StashFileID = "STSH"
const StatisticFileID = "STAT"
const DoltgresRootValueFileID = "DGRV"
const TupleFileID = "TUPL"
const VectorIndexNodeFileID = "IVFF"

const MessageTypesKind int = 27

const MessagePrefixSz = 4

type Message []byte

func GetFileID(bs []byte) string {
	if len(bs) < 8+MessagePrefixSz {
		return ""
	}
	return byteSliceToString(bs[MessagePrefixSz+4 : MessagePrefixSz+8])
}

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

// byteSliceToString converts a []byte to string without a heap allocation.
// copied from github.com/google/flatbuffers/go/sizes.go
func byteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
