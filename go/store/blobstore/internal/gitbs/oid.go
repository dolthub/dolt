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

package gitbs

import "fmt"

// validateOIDHex validates a 40-character hex object id.
//
// This is intentionally lenient about case (accepts A-F) since we may parse OIDs
// from sources that aren't normalized. Callers that require a canonical form should
// normalize separately (e.g. strings.ToLower).
func validateOIDHex(oid string) error {
	if len(oid) != 40 {
		return fmt.Errorf("expected 40 hex chars, got %d", len(oid))
	}
	for i := 0; i < len(oid); i++ {
		c := oid[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return fmt.Errorf("non-hex character %q", c)
		}
	}
	return nil
}
