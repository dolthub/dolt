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
	"unsafe"
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

func GetFileID(bs []byte) string {
	if len(bs) < 8 {
		return ""
	}
	return byteSliceToString(bs[4:8])
}

// byteSliceToString converts a []byte to string without a heap allocation.
// copied from github.com/google/flatbuffers/go/sizes.go
func byteSliceToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
