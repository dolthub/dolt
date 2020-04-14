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

package sqlserver

import (
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
)

type testPerson struct {
	Name       string
	Age        int
	Is_married bool
	Title      string
}

var (
	bill = testPerson{"Bill Billerson", 32, true, "Senior Dufus"}
	john = testPerson{"John Johnson", 25, false, "Dufus"}
	rob  = testPerson{"Rob Robertson", 21, false, ""}
)

func TestServerArgs(t *testing.T) {
	serverController := CreateServerController()
	go func() {
		startServer(context.Background(), "test", "dolt sql-server", []string{
			"-H", "localhost",
			"-P", "15200",
			"-u", "username",
			"-p", "password",
			"-t", "5",
			"-l", "info",
			"-r",
		}, createEnvWithSeedData(t), serverController)
	}()
	err := serverController.WaitForStart()
	require.NoError(t, err)
	conn, err := dbr.Open("mysql", "username:password@tcp(localhost:15200)/dolt", nil)
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)
	serverController.StopServer()
	err = serverController.WaitForClose()
	assert.NoError(t, err)
}

func TestServerBadArgs(t *testing.T) {
	env := createEnvWithSeedData(t)

	tests := [][]string{
		{"-H", "127.0.0.0.1"},
		{"-H", "loclahost"},
		{"-P", "300"},
		{"-P", "90000"},
		{"-u", ""},
		{"-t", "-1"},
		{"-l", "everything"},
	}

	for _, test := range tests {
		t.Run(strings.Join(test, " "), func(t *testing.T) {
			serverController := CreateServerController()
			go func(serverController *ServerController) {
				startServer(context.Background(), "test", "dolt sql-server", test, env, serverController)
			}(serverController)
			// In the event that a test fails, we need to prevent a test from hanging due to a running server
			err := serverController.WaitForStart()
			require.Error(t, err)
			serverController.StopServer()
			err = serverController.WaitForClose()
			assert.NoError(t, err)
		})
	}
}

func TestServerGoodParams(t *testing.T) {
	env := createEnvWithSeedData(t)

	tests := []*ServerConfig{
		DefaultServerConfig(),
		DefaultServerConfig().WithHost("127.0.0.1").WithPort(15400),
		DefaultServerConfig().WithHost("localhost").WithPort(15401),
		//DefaultServerConfig().WithHost("::1").WithPort(15402), // Fails on Jenkins, assuming no IPv6 support
		DefaultServerConfig().WithUser("testusername").WithPort(15403),
		DefaultServerConfig().WithPassword("hunter2").WithPort(15404),
		DefaultServerConfig().WithTimeout(0).WithPort(15405),
		DefaultServerConfig().WithTimeout(5).WithPort(15406),
		DefaultServerConfig().WithLogLevel(LogLevel_Debug).WithPort(15407),
		DefaultServerConfig().WithLogLevel(LogLevel_Info).WithPort(15408),
		DefaultServerConfig().WithReadOnly(true).WithPort(15409),
		DefaultServerConfig().WithUser("testusernamE").WithPassword("hunter2").WithTimeout(4).WithPort(15410),
	}

	for _, test := range tests {
		t.Run(test.String(), func(t *testing.T) {
			sc := CreateServerController()
			go func(config *ServerConfig, sc *ServerController) {
				_, _ = Serve(context.Background(), config, sc, env)
			}(test, sc)
			err := sc.WaitForStart()
			require.NoError(t, err)
			conn, err := dbr.Open("mysql", test.ConnectionString(), nil)
			require.NoError(t, err)
			err = conn.Close()
			require.NoError(t, err)
			sc.StopServer()
			err = sc.WaitForClose()
			assert.NoError(t, err)
		})
	}
}

func TestServerSelect(t *testing.T) {
	env := createEnvWithSeedData(t)
	serverConfig := DefaultServerConfig().WithLogLevel(LogLevel_Fatal).WithPort(15300)

	sc := CreateServerController()
	defer sc.StopServer()
	go func() {
		_, _ = Serve(context.Background(), serverConfig, sc, env)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	conn, err := dbr.Open("mysql", serverConfig.ConnectionString(), nil)
	require.NoError(t, err)
	defer conn.Close()
	sess := conn.NewSession(nil)

	tests := []struct {
		query       func() *dbr.SelectStmt
		expectedRes []testPerson
	}{
		{func() *dbr.SelectStmt { return sess.Select("*").From("people") }, []testPerson{bill, john, rob}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age = 32") }, []testPerson{bill}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("title = 'Senior Dufus'") }, []testPerson{bill}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name = 'Bill Billerson'") }, []testPerson{bill}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name = 'John Johnson'") }, []testPerson{john}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age = 25") }, []testPerson{john}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("25 = age") }, []testPerson{john}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("is_married = false") }, []testPerson{john, rob}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age < 30") }, []testPerson{john, rob}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age > 24") }, []testPerson{bill, john}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age >= 25") }, []testPerson{bill, john}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name <= 'John Johnson'") }, []testPerson{bill, john}},
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name <> 'John Johnson'") }, []testPerson{bill, rob}},
		{func() *dbr.SelectStmt {
			return sess.Select("age, is_married").From("people").Where("name = 'John Johnson'")
		}, []testPerson{{"", 25, false, ""}}},
	}

	for _, test := range tests {
		query := test.query()
		t.Run(query.Query, func(t *testing.T) {
			var peoples []testPerson
			_, err := query.LoadContext(context.Background(), &peoples)
			assert.NoError(t, err)
			assert.ElementsMatch(t, peoples, test.expectedRes)
		})
	}
}

func createEnvWithSeedData(t *testing.T) *env.DoltEnv {
	dEnv := dtestutils.CreateTestEnv()
	imt, sch := dtestutils.CreateTestDataTable(true)

	rd := table.NewInMemTableReader(imt)
	wr := noms.NewNomsMapCreator(context.Background(), dEnv.DoltDB.ValueReadWriter(), sch)

	_, _, err := table.PipeRows(context.Background(), rd, wr, false)
	rd.Close(context.Background())
	wr.Close(context.Background())

	if err != nil {
		t.Error("Failed to seed initial data", err)
	}

	err = dEnv.PutTableToWorking(context.Background(), *wr.GetMap(), wr.GetSchema(), "people")

	if err != nil {
		t.Error("Unable to put initial value of table in in mem noms db", err)
	}

	return dEnv
}
