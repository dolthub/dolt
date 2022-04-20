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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

const CommitFuncName = "commit"

// Deprecated: please use the version in the dprocedures package
type CommitFunc struct {
	children []sql.Expression
}

// NewCommitFunc creates a new CommitFunc expression.
// Deprecated: please use the version in the dprocedures package
func NewCommitFunc(args ...sql.Expression) (sql.Expression, error) {
	return &CommitFunc{children: args}, nil
}

// Eval implements the Expression interface.
func (cf *CommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, cf.Children())
	if err != nil {
		return noConflicts, err
	}
	return DoDoltCommit(ctx, args)
}

// String implements the Stringer interface.
func (cf *CommitFunc) String() string {
	childrenStrings := make([]string, len(cf.children))

	for i, child := range cf.children {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("COMMIT(%s)", strings.Join(childrenStrings, ","))
}

// IsNullable implements the Expression interface.
func (cf *CommitFunc) IsNullable() bool {
	return false
}

func (cf *CommitFunc) Resolved() bool {
	for _, child := range cf.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (cf *CommitFunc) Children() []sql.Expression {
	return cf.children
}

// WithChildren implements the Expression interface.
func (cf *CommitFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewCommitFunc(children...)
}

func (cf *CommitFunc) Type() sql.Type {
	return sql.Text
}
