package sqle

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
)

type DoltExecBuilder struct {
	rowexec.BaseBuilder
}

func NewDoltExecBuilder() sql.NodeExecBuilder {
	b := &DoltExecBuilder{rowexec.BaseBuilder{}}
	b.WithCustomSources(func(ctx *sql.Context, n sql.Node, row sql.Row) (sql.RowIter, error) {
		switch n := n.(type) {
		case *PatchTableFunction:
			return n.RowIter(ctx, row)
		case *LogTableFunction:
			return n.RowIter(ctx, row)
		case *DiffTableFunction:
			return n.RowIter(ctx, row)
		case *DiffSummaryTableFunction:
			return n.RowIter(ctx, row)
		case *DiffStatTableFunction:
			return n.RowIter(ctx, row)
		default:
			return nil, nil
		}
	})
	return b
}
