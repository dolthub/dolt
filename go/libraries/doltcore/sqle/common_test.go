// Copyright 2019 Liquidata, Inc.
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
	"context"
	"io"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Runs the query given and returns the result. The schema result of the query's execution is currently ignored, and
// the targetSchema given is used to prepare all rows.
func executeSelect(ctx context.Context, dEnv *env.DoltEnv, targetSch schema.Schema, root *doltdb.RootValue, query string) ([]row.Row, schema.Schema, error) {
	var err error
	db := NewDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState, dEnv.RepoStateWriter())
	engine, sqlCtx, err := NewTestEngine(ctx, db, root)
	if err != nil {
		return nil, nil, err
	}

	_, iter, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, nil, err
	}

	if targetSch == nil {
		return nil, nil, nil
	}

	doltRows := make([]row.Row, 0)
	var r sql.Row
	for r, err = iter.Next(); err == nil; r, err = iter.Next() {
		sqlR, err := SqlRowToDoltRow(types.Format_7_18, r, targetSch)

		if err != nil {
			return nil, nil, err
		}

		doltRows = append(doltRows, sqlR)
	}

	if err != io.EOF {
		return nil, nil, err
	}

	return doltRows, targetSch, nil
}

// Runs the query given and returns the error (if any).
func executeModify(ctx context.Context, ddb *doltdb.DoltDB, root *doltdb.RootValue, query string) (*doltdb.RootValue, error) {
	db := NewDatabase("dolt", root, ddb, nil, nil)
	engine, sqlCtx, err := NewTestEngine(ctx, db, root)

	if err != nil {
		return nil, err
	}

	_, _, err = engine.Query(sqlCtx, query)

	if err != nil {
		return nil, err
	}

	return db.GetRoot(sqlCtx)
}

func schemaNewColumn(t *testing.T, name string, tag uint64, sqlType sql.Type, partOfPK bool, constraints ...schema.ColConstraint) schema.Column {
	typeInfo, err := typeinfo.FromSqlType(sqlType)
	require.NoError(t, err)
	col, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, constraints...)
	require.NoError(t, err)
	return col
}
