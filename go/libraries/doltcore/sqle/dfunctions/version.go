// Copyright 2020 Dolthub, Inc.
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

package dfunctions

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const VersionFuncName = "dolt_version"

var VersionString = "SET_BY_INIT"

type Version struct{}

// NewVersion creates a new Version expression.
func NewVersion() sql.Expression {
	return &Version{}
}

// Children implements the Expression interface.
func (*Version) Children() []sql.Expression {
	return nil
}

// Eval implements the Expression interface.
func (*Version) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return VersionString, nil
}

// IsNullable implements the Expression interface.
func (*Version) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (*Version) Resolved() bool {
	return true
}

// String implements the Stringer interface.
func (*Version) String() string {
	return "DOLT_VERSION"
}

// Type implements the Expression interface.
func (*Version) Type() sql.Type {
	return types.Text
}

// WithChildren implements the Expression interface.
func (v *Version) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 0)
	}
	return NewVersion(), nil
}
