// Copyright 2019 Liquidata, Inc.
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

package sql

import (
	"strings"
)

// Error format string for reporting non-unique table names / aliases in a select
const NonUniqueTableNameErrFmt = "Non-unique table name / alias: '%v'"

// Aliases tracks the aliased identifiers in a query
type Aliases struct {
	// Table aliases by their name. Table aliases must be distinct and are case-insensitive
	tableAliases map[string]string
	// Column aliases by their name. Multiple columns can share the same alias.
	columnAliases map[string][]*RowValGetter
}

func NewAliases() *Aliases {
	return &Aliases{
		tableAliases:  make(map[string]string),
		columnAliases: make(map[string][]*RowValGetter),
	}
}

// Returns a copy of the aliases with only table aliases filled in.
func (a *Aliases) TableAliasesOnly() *Aliases {
	return &Aliases{
		tableAliases:  a.tableAliases,
		columnAliases: make(map[string][]*RowValGetter),
	}
}

// Adds a table alias as specified. Returns an error if the alias already exists
func (a *Aliases) AddTableAlias(tableName, alias string) error {
	lowerAlias := strings.ToLower(alias)
	if _, ok := a.tableAliases[lowerAlias]; ok {
		return errFmt(NonUniqueTableNameErrFmt, alias)
	}
	a.tableAliases[lowerAlias] = tableName

	return nil
}

// GetTableByAlias returns the table name with the case-insensitive alias given, and a boolean ok value.
func (a *Aliases) GetTableByAlias(alias string) (string, bool) {
	tableName, ok := a.tableAliases[strings.ToLower(alias)]
	return tableName, ok
}

// AddColumnAlias adds a column alias as specified. Multiple column aliases can share the same name
func (a *Aliases) AddColumnAlias(alias string, getter *RowValGetter) {
	lowerAlias := strings.ToLower(alias)
	a.columnAliases[lowerAlias] = append(a.columnAliases[lowerAlias], getter)
}

// GetColumn returns the slice of RowValGetters for the case-insensitive column alias named.
func (a *Aliases) GetColumnByAlias(alias string) []*RowValGetter {
	return a.columnAliases[strings.ToLower(alias)]
}
