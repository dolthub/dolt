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

package dfunctions

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const ActiveRevisionFuncName = "active_revision"

type ActiveRevisionFunc struct{}

// NewActiveRevisionFunc creates a new ActiveRevisionFunc expression.
func NewActiveRevisionFunc() sql.Expression {
	return &ActiveRevisionFunc{}
}

// Eval implements the Expression interface.
func (*ActiveRevisionFunc) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	_, activeRevision, err := resolveSessionDatabaseIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if activeRevision == "" {
		return nil, nil
	}
	return activeRevision, nil
}

// String implements the Stringer interface.
func (*ActiveRevisionFunc) String() string {
	return "ACTIVE_REVISION()"
}

// IsNullable implements the Expression interface.
func (*ActiveRevisionFunc) IsNullable() bool {
	return true
}

// Resolved implements the Expression interface.
func (*ActiveRevisionFunc) Resolved() bool {
	return true
}

// Type implements the Expression interface.
func (*ActiveRevisionFunc) Type() sql.Type {
	return types.Text
}

// Children implements the Expression interface.
func (*ActiveRevisionFunc) Children() []sql.Expression {
	return nil
}

// WithChildren implements the Expression interface.
func (f *ActiveRevisionFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return NewActiveRevisionFunc(), nil
}
