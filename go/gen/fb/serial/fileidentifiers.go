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
	return string(bs[4:8])
}
