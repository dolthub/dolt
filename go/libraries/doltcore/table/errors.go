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

package table

import (
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
)

// BadRow is an error which contains the row and details about what is wrong with it.
type BadRow struct {
	Row     row.Row
	Details []string
}

// NewBadRow creates a BadRow instance with a given row and error details
func NewBadRow(r row.Row, details ...string) *BadRow {
	return &BadRow{r, details}
}

// IsBadRow takes an error and returns whether it is a BadRow
func IsBadRow(err error) bool {
	_, ok := err.(*BadRow)

	return ok
}

// GetBadRow will retrieve the Row from the BadRow error
func GetBadRowRow(err error) row.Row {
	br, ok := err.(*BadRow)

	if !ok {
		panic("Call IsBadRow prior to trying to get the BadRowRow")
	}

	return br.Row
}

// Error returns a string with error details.
func (br *BadRow) Error() string {
	return strings.Join(br.Details, "\n")
}
