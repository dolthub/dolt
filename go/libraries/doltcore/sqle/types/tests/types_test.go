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

package tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"hash/fnv"
	"math/big"
	"strings"
	"testing"

	"github.com/gocraft/dbr/v2"
	sqlServer "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	dtypes "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/types"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Tests must be in this directory due to an import cycle with sqle and sqle/types

func TestSqlTypes(t *testing.T) {
	for _, sqlTypeInit := range dtypes.SqlTypeInitializers {
		t.Run(sqlTypeInit.String(), func(t *testing.T) {
			if sqlTypeInit.SqlType() == sql.Boolean {
				t.Skip("Skipping tests involving Boolean until that's changed in go-mysql-server")
			}
			for _, sqlType := range sqlTypeInit.SqlTypes() {
				sqlTypeStr, err := dtypes.SqlTypeToString(sqlType)
				require.NoError(t, err)
				root, dEnv := getEmptyRoot()
				root, err = runQuery(root, dEnv, fmt.Sprintf("CREATE TABLE test (pk BIGINT, v %v, PRIMARY KEY (pk))", sqlTypeStr))
				require.NoError(t, err)
				table, exists, err := root.GetTable(context.Background(), "test")
				require.NoError(t, err)
				require.True(t, exists)
				schema, err := table.GetSchema(context.Background())
				require.NoError(t, err)
				cols := schema.GetNonPKCols().GetColumns()
				require.Len(t, cols, 1)
				col := cols[0]
				require.Equal(t, sqlTypeInit.NomsKind(), col.Kind, "Expected %v on %v but got %v", sqlTypeInit.NomsKind(), sqlTypeInit, col.Kind)
			}
		})
	}
}

func closeServer(t *testing.T, conn *dbr.Connection, serverController *sqlserver.ServerController) {
	err := conn.Close()
	require.NoError(t, err)
	serverController.StopServer()
	err = serverController.WaitForClose()
	assert.NoError(t, err)
}

func hash(t *testing.T, s string) string {
	h := fnv.New64a()
	_, err := h.Write([]byte(s))
	require.NoError(t, err)
	return fmt.Sprintf("a%v", h.Sum64())
}

func roundTrip(t *testing.T, originalValue string, sqlType sql.Type, conn *dbr.Connection) {
	// Hash the value to get a (hopefully) unique table name
	tableName := hash(t, originalValue)

	// Create a table, insert a value into it, then read it back
	sqlTypeStr, err := dtypes.SqlTypeToString(sqlType)
	require.NoError(t, err)
	_, err = conn.Exec(fmt.Sprintf("CREATE TABLE %v (pk BIGINT PRIMARY KEY, v %v)", tableName, sqlTypeStr))
	require.NoError(t, err)
	_, err = conn.Exec(fmt.Sprintf(`INSERT INTO %v VALUES (1, "%v")`, tableName, originalValue))
	require.NoError(t, err)
	rows, err := conn.Query(fmt.Sprintf(`SELECT v FROM %v WHERE pk = 1`, tableName))
	require.NoError(t, err)
	require.True(t, rows.Next())
	fromServer := ""
	require.NoError(t, rows.Scan(&fromServer))

	// Get the CREATE TABLE statement as defined by the previous table
	rows, err = conn.Query(fmt.Sprintf(`SHOW CREATE TABLE %v`, tableName))
	require.NoError(t, err)
	require.True(t, rows.Next())
	createStatement := ""
	require.NoError(t, rows.Scan(&tableName, &createStatement))
	createStatement = strings.Replace(strings.ToLower(createStatement), tableName, tableName+"2", 1)
	if !strings.Contains(createStatement, "primary key") {
		createStatement = strings.Replace(createStatement, "not null", "not null primary key", 1)
	}

	// Create a table from the previous, insert the pulled value, and compare it to the original
	_, err = conn.Exec(createStatement)
	require.NoError(t, err)
	_, err = conn.Exec(fmt.Sprintf(`INSERT INTO %v2 VALUES (1, "%v")`, tableName, fromServer))
	require.NoError(t, err)
	rows, err = conn.Query(fmt.Sprintf(`SELECT v FROM %v2 WHERE pk = 1`, tableName))
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&fromServer))
	require.Equal(t, originalValue, fromServer)
}

// runQuery runs the given query and returns a new root value
func runQuery(root *doltdb.RootValue, dEnv *env.DoltEnv, query string) (*doltdb.RootValue, error) {
	db := sqle.NewDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState)
	engine := sqlServer.NewDefault()
	engine.AddDatabase(db)
	_ = engine.Init()
	sqlCtx := sql.NewContext(context.Background())
	_, _, err := engine.Query(sqlCtx, query)
	return db.Root(), err
}

func runServer(t *testing.T) (*dbr.Connection, *sqlserver.ServerController) {
	serverController := sqlserver.CreateServerController()
	port, err := rand.Int(rand.Reader, big.NewInt(1000))
	require.NoError(t, err)
	serverConfig := sqlserver.DefaultServerConfig().WithPort(16000 + int(port.Int64()))
	go func() {
		root, _ := getEmptyRoot()
		_, _ = sqlserver.Serve(context.Background(), serverConfig, root, serverController)
	}()
	err = serverController.WaitForStart()
	require.NoError(t, err)
	conn, err := dbr.Open("mysql", serverConfig.ConnectionString(), nil)
	if !assert.NoError(t, err) {
		serverController.StopServer()
		err = serverController.WaitForClose()
		require.NoError(t, err)
	}
	return conn, serverController
}

func testParse(t *testing.T, sqlVal interface{}, val types.Value, sqlType sql.Type) {
	sqlTypeStr, err := dtypes.SqlTypeToString(sqlType)
	require.NoError(t, err)
	root, dEnv := getEmptyRoot()
	root, err = runQuery(root, dEnv, fmt.Sprintf("CREATE TABLE test (pk BIGINT COMMENT 'tag:0', v %v COMMENT 'tag:1', PRIMARY KEY (pk))", sqlTypeStr))
	require.NoError(t, err)
	if valString, ok := sqlVal.(string); ok {
		root, err = runQuery(root, dEnv, fmt.Sprintf(`INSERT INTO test VALUES (1, "%v")`, valString))
	} else {
		root, err = runQuery(root, dEnv, fmt.Sprintf("INSERT INTO test VALUES (1, %v)", sqlVal))
	}
	require.NoError(t, err)
	table, tableExists, err := root.GetTable(context.Background(), "test")
	require.NoError(t, err)
	require.True(t, tableExists)
	schema, err := table.GetSchema(context.Background())
	require.NoError(t, err)
	dRow, err := row.New(table.Format(), schema, map[uint64]types.Value{0: types.Int(1)})
	require.NoError(t, err)
	nmkValue, err := dRow.NomsMapKey(schema).Value(context.Background())
	require.NoError(t, err)
	rowData, rowExists, err := table.GetRow(context.Background(), nmkValue.(types.Tuple), schema)
	require.NoError(t, err)
	require.True(t, rowExists)
	colVal, _ := rowData.GetColVal(1)
	require.True(t, val.Equals(colVal))
}

func getEmptyRoot() (*doltdb.RootValue, *env.DoltEnv) {
	dEnv := dtestutils.CreateTestEnv()
	root, _ := dEnv.WorkingRoot(context.Background())
	return root, dEnv
}
