// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed toIter in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querydiff

import (
	"errors"
	"fmt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/diff"
	"io"

	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"

	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
)

var errSkip = errors.New("errSkip") // u lyk hax?

type QueryDiffer struct {
	sch      sql.Schema
	fromIter sql.RowIter
	toIter   sql.RowIter
}

func MakeQueryDiffer(fromCtx, toCtx *sql.Context, fromEng, toEng *sqle.Engine, query string) (*QueryDiffer, error) {
	// todo: make the sql engines here

	from, to, err := hackThatPlan(fromCtx, toCtx, fromEng, toEng, query)
	if err != nil {
		return nil, err
	}

	fromIter, err := from.RowIter(fromCtx)
	if err != nil {
		return nil, err
	}
	toIter, err := to.RowIter(toCtx)
	if err != nil {
		return nil, err
	}

	fmt.Printf("%s\n", diff.To)

	_ = dsqle.Database{}

	qd := &QueryDiffer{
		sch:      from.Schema(),
		fromIter: fromIter,
		toIter:   toIter,
	}

	return qd, nil
}

func (qd *QueryDiffer) NextDiff() (from sql.Row, to sql.Row, err error) {
	var fromEOF bool
	for {
		from, err = qd.fromIter.Next()
		if err == io.EOF {
			fromEOF = true
		} else if err != nil && err != errSkip {
			return nil, nil, err
		}

		to, err = qd.toIter.Next()
		if err != nil && err != errSkip && err != io.EOF {
			return nil, nil, err
		}

		if fromEOF && err == io.EOF {
			return nil, nil, io.EOF
		}

		eq, err := from.Equals(to, qd.sch)
		if err != nil {
			return nil, nil, err
		}
		if eq {
			continue
		}

		return from, to, nil
	}
}

func (qd *QueryDiffer) Schema() sql.Schema {
	return qd.sch
}

func (qd *QueryDiffer) Close() error {
	fromErr := qd.fromIter.Close()
	toErr := qd.toIter.Close()
	if fromErr != nil {
		return fromErr
	}
	return toErr
}

type NodeDiffer interface {
	FromNode() sql.Node
	ToNode() sql.Node
}

// todo: consult Engine.Query() for logic re: perms, catalog
func hackThatPlan(fromCtx *sql.Context, toCtx *sql.Context, fromEng *sqle.Engine, toEng *sqle.Engine, query string) (fromPlan, toPlan sql.Node, err error) {
	parsed, err := parse.Parse(fromCtx, query)
	if err != nil {
		return nil, nil, err
	}

	fromPlan, err = fromEng.Analyzer.Analyze(fromCtx, parsed)
	if err != nil {
		return nil, nil, err
	}
	toPlan, err = toEng.Analyzer.Analyze(toCtx, parsed)
	if err != nil {
		return nil, nil, err
	}

	fromPlan, toPlan, err = recurseModifyPlans(fromCtx, toCtx, fromPlan, toPlan)
	if err != nil {
		return nil, nil, err
	}

	return fromPlan, toPlan, nil
}

func recurseModifyPlans(fromCtx, toCtx *sql.Context, from, to sql.Node) (modFrom, modTo sql.Node, err error) {
	// todo: don't assume plans are equal
	switch from.(type) {
	case *plan.Sort:
		nd, err := newSortNodeDiffer(fromCtx, toCtx, from.(*plan.Sort), to.(*plan.Sort))
		if err != nil {
			return nil, nil, err
		}
		modFrom, modTo = nd.FromNode(), nd.ToNode()
	default:
		fc := from.Children()
		tc := to.Children()
		if fc == nil || tc == nil {
			return nil, nil, fmt.Errorf("reached bottom of query plan")
		}
		fc[0], tc[0], err = recurseModifyPlans(fromCtx, toCtx, fc[0], tc[0])
		if err != nil {
			return nil, nil, err
		}
		modFrom, err = from.WithChildren(fc...)
		if err != nil {
			return nil, nil, err
		}
		modTo, err = to.WithChildren(tc...)
		if err != nil {
			return nil, nil, err
		}
	}
	return modFrom, modTo, nil
}
