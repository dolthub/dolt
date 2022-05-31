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
	goerrors "gopkg.in/src-d/go-errors.v1"
)

const MergeFuncName = "merge"

var ErrUncommittedChanges = goerrors.NewKind("cannot merge with uncommitted changes")

// Deprecated: please use the version in the dprocedures package
type MergeFunc struct {
	children []sql.Expression
}

// NewMergeFunc creates a new MergeFunc expression.
// Deprecated: please use the version in the dprocedures package
func NewMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &MergeFunc{children: args}, nil
}

// Eval implements the Expression interface.
func (mf *MergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, mf.Children())
	if err != nil {
		return noConflictsOrViolations, err
	}
	return DoDoltMerge(ctx, args)
}

// String implements the Stringer interface.
func (mf *MergeFunc) String() string {
	childrenStrings := make([]string, len(mf.children))

	for i, child := range mf.children {
		childrenStrings[i] = child.String()
	}
	return fmt.Sprintf("Merge(%s)", strings.Join(childrenStrings, ","))
}

// IsNullable implements the Expression interface.
func (mf *MergeFunc) IsNullable() bool {
	return false
}

func (mf *MergeFunc) Resolved() bool {
	for _, child := range mf.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (mf *MergeFunc) Children() []sql.Expression {
	return mf.children
}

// WithChildren implements the Expression interface.
func (mf *MergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewMergeFunc(children...)
}

// Type implements the Expression interface.
func (mf *MergeFunc) Type() sql.Type {
	return sql.Text
}
