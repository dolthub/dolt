package dfunctions

import (
	"github.com/dolthub/go-mysql-server/sql"
)

const DoltCommitFuncName = "dolt_commit"

type DoltCommitFunc struct {
	children []sql.Expression
}

// NewDoltCommitFunc creates a new DoltCommitFunc expression.
func NewDoltCommitFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltCommitFunc{children: args}, nil
}

func (d DoltCommitFunc) String() string {
	panic("implement me")
}

func (d DoltCommitFunc) Type() sql.Type {
	panic("implement me")
}

func (d DoltCommitFunc) IsNullable() bool {
	panic("implement me")
}

func (d DoltCommitFunc) Eval(context *sql.Context, row sql.Row) (interface{}, error) {
	panic("implement me")
}

func (d DoltCommitFunc) WithChildren(expression ...sql.Expression) (sql.Expression, error) {
	panic("implement me")
}


func (d DoltCommitFunc) Resolved() bool {
	panic("implement me")
}

func (d DoltCommitFunc) Children() []sql.Expression {
	panic("implement me")
}
