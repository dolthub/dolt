// Copyright 2024 Dolthub, Inc.
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

package kvexec

import (
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
)

func newCountAggregationKvIter(srcIter prolly.MapIter, sch schema.Schema, e sql.Expression) (sql.RowIter, bool, error) {
	var nullable bool
	var idx int
	var isKeyRef bool

	switch e := e.(type) {
	case sql.LiteralExpression:
		nullable = false
	case *expression.GetField:
		// name -> tag -> position in source key/val
		col, ok := sch.GetAllCols().LowerNameToCol[strings.ToLower(e.Name())]
		if !ok || col.Virtual {
			return nil, false, nil
		}
		nullable = col.IsNullable()
		if col.IsPartOfPK {
			isKeyRef = true
			idx, _ = sch.GetPKCols().StoredIndexByTag(col.Tag)
		} else {
			idx, _ = sch.GetNonPKCols().StoredIndexByTag(col.Tag)
		}
	default:
		return nil, false, nil
		// todo: tuple
	}

	return &countAggKvIter{
		srcIter:  srcIter,
		nullable: nullable,
		isKeyRef: isKeyRef,
		idx:      idx,
	}, true, nil
}

type countAggKvIter struct {
	srcIter  prolly.MapIter
	nullable bool
	isKeyRef bool
	idx      int
	done     bool
}

func (l *countAggKvIter) Close(_ *sql.Context) error {
	return nil
}

func (l *countAggKvIter) Next(ctx *sql.Context) (sql.Row, error) {
	// will return one value
	if l.done {
		return nil, io.EOF
	}
	var cnt int64
	for {
		k, v, err := l.srcIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if l.nullable {
			if l.isKeyRef && k.FieldIsNull(l.idx) ||
				v.FieldIsNull(l.idx) {
				continue
			}
		}
		cnt++
	}
	l.done = true
	return sql.Row{cnt}, nil
}
