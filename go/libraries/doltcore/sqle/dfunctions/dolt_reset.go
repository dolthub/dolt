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

import "github.com/dolthub/go-mysql-server/sql"

const DoltResetFuncName = "dolt_reset"

type DoltResetFunc struct {
	children []sql.Expression
}

func (d DoltResetFunc) Resolved() bool {
	panic("implement me")
}

func (d DoltResetFunc) String() string {
	panic("implement me")
}

func (d DoltResetFunc) Type() sql.Type {
	panic("implement me")
}

func (d DoltResetFunc) IsNullable() bool {
	panic("implement me")
}

func (d DoltResetFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("implement me")
}

func (d DoltResetFunc) Children() []sql.Expression {
	panic("implement me")
}

func (d DoltResetFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	panic("implement me")
}

func NewDoltResetFunc(args ...sql.Expression) (sql.Expression, error) {
	return DoltResetFunc{children: args}, nil
}