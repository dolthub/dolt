// Copyright 2023 Dolthub, Inc.
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
