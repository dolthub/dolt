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

package dtables

import (
	"strings"

	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// ColumnPredicate returns a predicate function for expressions on the column names given
func ColumnPredicate(colNameSet *set.StrSet) func(sql.Expression) bool {
	return func(filter sql.Expression) bool {
		isCommitFilter := true
		sql.Inspect(filter, func(e sql.Expression) (cont bool) {
			if e == nil {
				return true
			}

			switch val := e.(type) {
			case *expression.GetField:
				if !colNameSet.Contains(strings.ToLower(val.Name())) {
					isCommitFilter = false
					return false
				}
			}

			return true
		})

		return isCommitFilter
	}
}

// FilterFilters returns the subset of the expressions given that match the given predicate
func FilterFilters(filters []sql.Expression, predicate func(filter sql.Expression) bool) []sql.Expression {
	matching := make([]sql.Expression, 0, len(filters))
	for _, f := range filters {
		if predicate(f) {
			matching = append(matching, f)
		}
	}
	return matching
}

