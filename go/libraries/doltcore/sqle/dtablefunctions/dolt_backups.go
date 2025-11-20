// Copyright 2025 Dolthub, Inc.
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
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

type BackupsTableFunction struct {
	ctx           *sql.Context
	database      sql.Database
	argumentExprs []sql.Expression
}

var _ sql.TableFunction = (*BackupsTableFunction)(nil)
var _ sql.ExecSourceRel = (*BackupsTableFunction)(nil)

var backupsTableSchema = sql.Schema{
	&sql.Column{Name: "name", Type: types.Text, PrimaryKey: true, Nullable: false},
	&sql.Column{Name: "url", Type: types.Text, PrimaryKey: false, Nullable: false},
}

func (btf *BackupsTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &BackupsTableFunction{
		ctx:      ctx,
		database: database,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (btf *BackupsTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	args, err := getDoltArgs(ctx, btf.argumentExprs, btf.Name(), row)
	if err != nil {
		return nil, err
	}

	apr, err := cli.CreateBackupArgParser().Parse(args)
	if err != nil {
		return nil, sql.ErrInvalidArgumentDetails.New(btf.Name(), err.Error())
	}

	showVerbose := apr.Contains(cli.VerboseFlag)

	sqlDb, ok := btf.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", btf.database)
	}

	dbName := sqlDb.Name()
	if len(dbName) == 0 {
		return nil, fmt.Errorf("empty database name")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	backups, err := dbData.Rsr.GetBackups()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0)
	remotes := map[string]env.Remote{}

	backups.Iter(func(key string, val env.Remote) bool {
		names = append(names, key)
		remotes[key] = val
		return true
	})

	sort.Strings(names)

	return &backupsItr{
		names:       names,
		remotes:     remotes,
		showVerbose: showVerbose,
		idx:         0,
	}, nil
}

func (btf *BackupsTableFunction) Schema() sql.Schema {
	ap := cli.CreateBackupArgParser()
	var lits []string
	// This is called at plan time, so we can only evaluate constant literals
	for _, expr := range btf.argumentExprs {
		if !expr.Resolved() {
			continue
		}

		lit, ok := expr.(*expression.Literal)
		if !ok {
			continue
		}

		val, _ := lit.Eval(nil, nil)
		str, ok := val.(string)
		if !ok {
			continue
		}
		lits = append(lits, str)
	}

	schema := make(sql.Schema, len(backupsTableSchema))
	copy(schema, backupsTableSchema)

	apr, _ := ap.Parse(lits)
	if apr.Contains(cli.VerboseFlag) {
		schema = append(schema, &sql.Column{Name: "params", Type: types.JSON, PrimaryKey: false, Nullable: true})
	}
	return schema
}

func (btf *BackupsTableFunction) Resolved() bool {
	for _, expr := range btf.argumentExprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (btf *BackupsTableFunction) String() string {
	var args []string
	for _, expr := range btf.argumentExprs {
		args = append(args, expr.String())
	}
	if len(args) == 0 {
		return "DOLT_BACKUPS()"
	}
	return fmt.Sprintf("DOLT_BACKUPS(%s)", strings.Join(args, ", "))
}

func (btf *BackupsTableFunction) Children() []sql.Node {
	return nil
}

func (btf *BackupsTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return btf, nil
}

func (btf *BackupsTableFunction) IsReadOnly() bool {
	return true
}

func (btf *BackupsTableFunction) Expressions() []sql.Expression {
	return btf.argumentExprs
}

func (btf *BackupsTableFunction) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(btf.Name(), "0 to 1", len(expressions))
	}

	newBtf := *btf
	newBtf.argumentExprs = expressions

	return &newBtf, nil
}

func (btf *BackupsTableFunction) Name() string {
	return "dolt_backups"
}

func (btf *BackupsTableFunction) Database() sql.Database {
	return btf.database
}

func (btf *BackupsTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	new := *btf
	new.database = database
	return &new, nil
}

type backupsItr struct {
	names       []string
	remotes     map[string]env.Remote
	showVerbose bool
	idx         int
}

var _ sql.RowIter = (*backupsItr)(nil)

func (bi *backupsItr) Next(ctx *sql.Context) (sql.Row, error) {
	if bi.idx >= len(bi.names) {
		return nil, io.EOF
	}

	name := bi.names[bi.idx]
	remote := bi.remotes[name]
	bi.idx++

	if bi.showVerbose {
		params, _, err := types.JSON.Convert(ctx, remote.Params)
		if err != nil {
			return nil, err
		}
		return sql.NewRow(name, remote.Url, params), nil
	}

	return sql.NewRow(name, remote.Url), nil
}

func (bi *backupsItr) Close(_ *sql.Context) error {
	return nil
}
