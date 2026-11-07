// Copyright 2026 Dolthub, Inc.
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

package constants

import "slices"

// EncryptedFormatString is the format string of an encrypted database, which only Dolt Enterprise reads.
const EncryptedFormatString = "__ENCRYPTED__DOLT__"

// doltFormatAliases are other format strings of the Dolt storage format. Dolt Enterprise adds EncryptedFormatString.
var doltFormatAliases []string

// IsDoltFormatString reports whether |s| names the Dolt storage format.
func IsDoltFormatString(s string) bool {
	return s == FormatDoltString || slices.Contains(doltFormatAliases, s)
}
