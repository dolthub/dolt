// Copyright 2020-2025 Dolthub, Inc.
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

package dtablefunctions

import (
	"fmt"
	"github.com/dolthub/dolt/go/store/prolly/tree"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
)

const jsonDiffTableDefaultRowCount = 1000

var _ sql.TableFunction = (*DiffTableFunction)(nil)
var _ sql.ExecSourceRel = (*DiffTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*DiffTableFunction)(nil)

// JsonDiffTableFunction implements the DOLT_JSON_DIFF table function.
// It takes two arguments, which it interprets as JSON objects.
// Each row of the result table represents a key that has changed between the two documents.
type JsonDiffTableFunction struct {
	fromExpr sql.Expression
	toExpr   sql.Expression
	database sql.Database
}

// NewInstance creates a new instance of TableFunction interface
func (dtf *JsonDiffTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &JsonDiffTableFunction{
		database: database,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (dtf *JsonDiffTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(dtf.Schema())
	numRows, _, err := dtf.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (dtf *JsonDiffTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return jsonDiffTableDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (dtf *JsonDiffTableFunction) Database() sql.Database {
	return dtf.database
}

// WithDatabase implements the sql.Databaser interface
func (dtf *JsonDiffTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ndtf := *dtf
	ndtf.database = database
	return &ndtf, nil
}

// Expressions implements the sql.Expressioner interface
func (dtf *JsonDiffTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{dtf.fromExpr, dtf.toExpr}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface
func (dtf *JsonDiffTableFunction) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(dtf, len(expressions), 2)
	}
	newDtf := *dtf
	newDtf.fromExpr = expressions[0]
	newDtf.toExpr = expressions[1]
	return &newDtf, nil
}

// Children implements the sql.Node interface
func (dtf *JsonDiffTableFunction) Children() []sql.Node {
	return nil
}

// RowIter implements the sql.Node interface
func (dtf *JsonDiffTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {

	fromValue, err := dtf.fromExpr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	fromJson, _, err := gmstypes.JSON.Convert(ctx, fromValue)
	if err != nil {
		return nil, err
	}

	toValue, err := dtf.toExpr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	toJson, _, err := gmstypes.JSON.Convert(ctx, toValue)
	if err != nil {
		return nil, err
	}

	differ, err := tree.NewJsonDiffer(ctx, fromJson.(sql.JSONWrapper), toJson.(sql.JSONWrapper))
	if err != nil {
		return nil, err
	}

	return jsonDiffRowIter{differ}, nil
}

type jsonDiffRowIter struct {
	differ tree.JsonDiffer
}

func (j jsonDiffRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	jsonDiff, err := j.differ.Next(ctx)
	if err != nil {
		return nil, err
	}
	mySqlJsonPath := tree.MySqlJsonPathFromKey(jsonDiff.Key)
	return sql.NewRow(jsonDiff.Type.DiffTypeString(), mySqlJsonPath, jsonDiff.From, jsonDiff.To), nil
}

func (j jsonDiffRowIter) Close(ctx *sql.Context) error {
	return nil
}

var _ sql.RowIter = jsonDiffRowIter{}

// WithChildren implements the sql.Node interface
func (dtf *JsonDiffTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return dtf, nil
}

var jsonDiffTableSchema = sql.Schema{
	&sql.Column{Name: "diff_type", Type: gmstypes.Text},
	&sql.Column{Name: "path", Type: gmstypes.Text},
	&sql.Column{Name: "from_value", Type: gmstypes.JSON},
	&sql.Column{Name: "to_value", Type: gmstypes.JSON},
}

// Schema implements the sql.Node interface
func (dtf *JsonDiffTableFunction) Schema() sql.Schema {
	return jsonDiffTableSchema
}

// Resolved implements the sql.Resolvable interface
func (dtf *JsonDiffTableFunction) Resolved() bool {
	return dtf.fromExpr.Resolved() && dtf.toExpr.Resolved()
}

func (dtf *JsonDiffTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (dtf *JsonDiffTableFunction) String() string {
	return fmt.Sprintf("DOLT_JSON_DIFF(%s, %s)", dtf.fromExpr.String(), dtf.toExpr.String())
}

// Name implements the sql.TableFunction interface
func (dtf *JsonDiffTableFunction) Name() string {
	return "dolt_json_diff"
}
