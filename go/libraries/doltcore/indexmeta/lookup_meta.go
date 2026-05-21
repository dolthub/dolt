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

package indexmeta

import "github.com/dolthub/go-mysql-server/sql"

// LookupMeta describes an index that can be used for a strict (unique-key) lookup.
// Kept in its own package so dsess can reference the type without importing sqle/index,
// which transitively pulls in the full go-mysql-server query planner.
type LookupMeta struct {
	Idx      sql.Index
	Fds      *sql.FuncDepSet
	Cols     sql.FastIntSet
	Ordinals []int
}
