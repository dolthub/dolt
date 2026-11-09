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

import (
	"slices"
	"strings"
)

// EncryptedFormatPrefix begins the format string of every encrypted database. Each implementation of encryption names
// its own format under it, and a store refuses a manifest in another format. Dolt opens none of them.
const EncryptedFormatPrefix = "__ENCRYPTED__"

// IsEncryptedFormatString reports whether |s| names an encrypted format.
func IsEncryptedFormatString(s string) bool {
	return strings.HasPrefix(s, EncryptedFormatPrefix)
}

// doltFormatAliases are other format strings of the Dolt storage format. Dolt Enterprise adds its encrypted format.
var doltFormatAliases []string

// IsDoltFormatString reports whether |s| names the Dolt storage format.
func IsDoltFormatString(s string) bool {
	return s == FormatDoltString || slices.Contains(doltFormatAliases, s)
}
