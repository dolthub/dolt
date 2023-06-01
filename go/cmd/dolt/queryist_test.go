package main

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"
)

type testcase struct {
	query               string
	expectedResult      []interface{}
	expectedSchemaTypes []sql.Type
}

var tests = []testcase{
	{
		query: "show create table data",
	},
	{
		query:               "select bit1 from data",
		expectedResult:      []interface{}{"!"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select integer2 from data",
		expectedResult:      []interface{}{"2"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select smallint3 from data",
		expectedResult:      []interface{}{"3"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select float_4 from data",
		expectedResult:      []interface{}{"4"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select double5 from data",
		expectedResult:      []interface{}{"5"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select bigInt6 from data",
		expectedResult:      []interface{}{"6"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select bool7 from data",
		expectedResult:      []interface{}{"1"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select tinyint8 from data",
		expectedResult:      []interface{}{"8"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select smallint9 from data",
		expectedResult:      []interface{}{"9"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select mediumint10 from data",
		expectedResult:      []interface{}{"10"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select decimal11 from data",
		expectedResult:      []interface{}{"11.01230"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select date12, time13, datetime14 from data",
		expectedResult:      []interface{}{"2023-05-31", "18:45:39", "2023-05-31 18:45:39"},
		expectedSchemaTypes: []sql.Type{types.LongText, types.LongText, types.LongText},
	},
	{
		query:               "select year15 from data",
		expectedResult:      []interface{}{"2015"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select char16 from data",
		expectedResult:      []interface{}{"char16"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select varchar17 from data",
		expectedResult:      []interface{}{"varchar17"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select binary18 from data",
		expectedResult:      []interface{}{"binary--18"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:               "select varbinary19 from data",
		expectedResult:      []interface{}{"vbinary19"},
		expectedSchemaTypes: []sql.Type{types.LongText},
	},
	{
		query:          "select json20 from data",
		expectedResult: []interface{}{`[1, 2, 3, "four", {"a": 1, "b": "2", "c": 3, "d": {"e": 1, "f": 2, "g": 3}}]`},
	},
}
var setupScripts = []string{
	`create table data (
		id BIGINT primary key,
		bit1 BIT(6),
		integer2 INTEGER,
		smallint3 SMALLINT,
		float_4 FLOAT,
		double5 DOUBLE,
		bigInt6 BIGINT,
		bool7 BOOLEAN,
		tinyint8 TINYINT,
		smallint9 SMALLINT,
		mediumint10 MEDIUMINT,
		decimal11 DECIMAL(10, 5),
		date12 DATE,
		time13 TIME,
		datetime14 DATETIME,
		year15 YEAR,
		char16 CHAR(10),
		varchar17 VARCHAR(10),
		binary18 BINARY(10),
		varbinary19 VARBINARY(10),
		json20 JSON
	 );`,

	`insert into data values (
		100, 33, 2, 3, 4, 5, 6,
		true, 8, 9, 10, 11.0123, "2023-05-31", "18:45:39", "2023-05-31 18:45:39",
		2015, "char16", "varchar17", "binary--18", "vbinary19",
		"[ 1 , 2 , 3 , ""four"" , { ""a"": 1, ""b"": ""2"", ""c"": 3, ""d"": { ""e"": 1, ""f"": 2, ""g"": 3 } }]"
		);`,
}

func TestQueryistCases(t *testing.T) {
	for _, test := range tests {
		RunSingleTest(t, test)
	}
}

func RunSingleTest(t *testing.T, test testcase) {
	t.Run(test.query+"-SqlEngineQueryist", func(t *testing.T) {
		// setup server engine
		ctx := context.Background()
		dEnv := dtestutils.CreateTestEnv()
		defer dEnv.DoltDB.Close()
		sqlEngine, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
		require.NoError(t, err)
		sqlCtx, err := sqlEngine.NewLocalContext(ctx)
		require.NoError(t, err)
		sqlCtx.SetCurrentDatabase(dbName)
		queryist := commands.NewSqlEngineQueryist(sqlEngine)

		// initialize server
		initServer(t, sqlCtx, queryist)

		// run test
		runTestcase(t, sqlCtx, queryist, test)
	})
	t.Run(test.query+"-ConnectionQueryist", func(t *testing.T) {
		// setup server
		dEnv, sc, serverConfig := startServer(t, true, "", "")
		err := sc.WaitForStart()
		require.NoError(t, err)
		defer dEnv.DoltDB.Close()
		conn, _ := newConnection(t, serverConfig)
		queryist := sqlserver.NewConnectionQueryist(conn)
		ctx := context.TODO()
		sqlCtx := sql.NewContext(ctx)

		// initialize server
		initServer(t, sqlCtx, queryist)

		// run test
		runTestcase(t, sqlCtx, queryist, test)

		// close server
		require.NoError(t, conn.Close())
		sc.StopServer()
		err = sc.WaitForClose()
		require.NoError(t, err)
	})
}

func runTestcase(t *testing.T, sqlCtx *sql.Context, queryist cli.Queryist, test testcase) {
	// run test
	schema, rowIter, err := queryist.Query(sqlCtx, test.query)
	require.NoError(t, err)

	// get a row of data
	row, err := rowIter.Next(sqlCtx)
	require.NoError(t, err)

	if len(test.expectedResult) > 0 {
		// test result row
		assert.Equal(t, len(test.expectedResult), len(row))

		for i, val := range row {
			expected := test.expectedResult[i]
			assert.Equal(t, expected, val)
		}
	} else {
		// log result row
		sb := strings.Builder{}
		for i, val := range row {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%v", val))
		}
		t.Logf("result: %s", sb.String())
	}

	// test result schema
	if len(test.expectedSchemaTypes) > 0 {
		for i, col := range schema {
			expected := test.expectedSchemaTypes[i]
			actual := col.Type
			assert.Equal(t, expected, actual,
				"schema type mismatch for column %s: %s != %s", col.Name, expected.String(), actual.String())
		}
	}
}

func initServer(t *testing.T, sqlCtx *sql.Context, queryist cli.Queryist) {
	for _, setupScript := range setupScripts {
		_, rowIter, err := queryist.Query(sqlCtx, setupScript)
		require.NoError(t, err)

		for {
			row, err := rowIter.Next(sqlCtx)
			if err == io.EOF {
				break
			} else {
				require.NoError(t, err)
				require.NotNil(t, row)
			}
		}
	}
}

// startServer will start sql-server with given host, unix socket file path and whether to use specific port, which is defined randomly.
func startServer(t *testing.T, withPort bool, host string, unixSocketPath string) (*env.DoltEnv, *sqlserver.ServerController, sqlserver.ServerConfig) {
	dEnv := dtestutils.CreateTestEnv()
	serverConfig := sqlserver.DefaultServerConfig()

	if withPort {
		rand.Seed(time.Now().UnixNano())
		port := 15403 + rand.Intn(25)
		serverConfig = serverConfig.WithPort(port)
	}
	if host != "" {
		serverConfig = serverConfig.WithHost(host)
	}
	if unixSocketPath != "" {
		serverConfig = serverConfig.WithSocket(unixSocketPath)
	}

	sc := sqlserver.NewServerController()
	go func() {
		_, _ = sqlserver.Serve(context.Background(), "0.0.0", serverConfig, sc, dEnv)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	return dEnv, sc, serverConfig
}

// newConnection takes sqlserver.serverConfig and opens a connection, and will return that connection with a new session
func newConnection(t *testing.T, serverConfig sqlserver.ServerConfig) (*dbr.Connection, *dbr.Session) {
	const dbName = "dolt"
	conn, err := dbr.Open("mysql", sqlserver.ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess := conn.NewSession(nil)
	return conn, sess
}
