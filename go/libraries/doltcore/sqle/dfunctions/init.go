package dfunctions

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression/function"
)

func init() {
	function.Defaults = append(function.Defaults, sql.Function1{Name: HashOfFuncName, Fn: NewHashOf})
	function.Defaults = append(function.Defaults, sql.Function1{Name: CommitFuncName, Fn: NewCommitFunc})
}
