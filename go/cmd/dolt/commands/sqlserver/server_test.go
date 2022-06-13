// Copyright 2019 Dolthub, Inc.
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
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

//TODO: server tests need to expose a higher granularity for server interactions:
// - replication readers and writers that are connected through configs
// - configs need to be dynamic
// - interleave inter and intra-session queries
// - simulate server/connection failures
// - load balancing?
// - multi-master?

type testPerson struct {
	Name       string
	Age        int
	Is_married bool
	Title      string
}

type testBranch struct {
	Branch string
}

var (
	bill = testPerson{"Bill Billerson", 32, true, "Senior Dufus"}
	john = testPerson{"John Johnson", 25, false, "Dufus"}
	rob  = testPerson{"Rob Robertson", 21, false, ""}
)

func TestServerArgs(t *testing.T) {
	serverController := NewServerController()
	go func() {
		startServer(context.Background(), "test", "dolt sql-server", []string{
			"-H", "localhost",
			"-P", "15200",
			"-u", "username",
			"-p", "password",
			"-t", "5",
			"-l", "info",
			"-r",
		}, dtestutils.CreateEnvWithSeedData(t), serverController)
	}()
	err := serverController.WaitForStart()
	require.NoError(t, err)
	conn, err := dbr.Open("mysql", "username:password@tcp(localhost:15200)/", nil)
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)
	serverController.StopServer()
	err = serverController.WaitForClose()
	assert.NoError(t, err)
}

func TestYAMLServerArgs(t *testing.T) {
	const yamlConfig = `
log_level: info

behavior:
    read_only: true

user:
    name: username
    password: password

listener:
    host: localhost
    port: 15200
    read_timeout_millis: 5000
    write_timeout_millis: 5000
`
	serverController := NewServerController()
	go func() {
		dEnv := dtestutils.CreateEnvWithSeedData(t)
		dEnv.FS.WriteFile("config.yaml", []byte(yamlConfig))
		startServer(context.Background(), "test", "dolt sql-server", []string{
			"--config", "config.yaml",
		}, dEnv, serverController)
	}()
	err := serverController.WaitForStart()
	require.NoError(t, err)
	conn, err := dbr.Open("mysql", "username:password@tcp(localhost:15200)/", nil)
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)
	serverController.StopServer()
	err = serverController.WaitForClose()
	assert.NoError(t, err)
}

func TestServerBadArgs(t *testing.T) {
	env := dtestutils.CreateEnvWithSeedData(t)

	tests := [][]string{
		{"-H", "127.0.0.0.1"},
		{"-H", "loclahost"},
		{"-P", "300"},
		{"-P", "90000"},
		{"-u", ""},
		{"-l", "everything"},
	}

	for _, test := range tests {
		t.Run(strings.Join(test, " "), func(t *testing.T) {
			serverController := NewServerController()
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
	env := dtestutils.CreateEnvWithSeedData(t)

	tests := []ServerConfig{
		DefaultServerConfig(),
		DefaultServerConfig().withHost("127.0.0.1").WithPort(15400),
		DefaultServerConfig().withHost("localhost").WithPort(15401),
		//DefaultServerConfig().withHost("::1").WithPort(15402), // Fails on Jenkins, assuming no IPv6 support
		DefaultServerConfig().withUser("testusername").WithPort(15403),
		DefaultServerConfig().withPassword("hunter2").WithPort(15404),
		DefaultServerConfig().withTimeout(0).WithPort(15405),
		DefaultServerConfig().withTimeout(5).WithPort(15406),
		DefaultServerConfig().withLogLevel(LogLevel_Debug).WithPort(15407),
		DefaultServerConfig().withLogLevel(LogLevel_Info).WithPort(15408),
		DefaultServerConfig().withReadOnly(true).WithPort(15409),
		DefaultServerConfig().withUser("testusernamE").withPassword("hunter2").withTimeout(4).WithPort(15410),
	}

	for _, test := range tests {
		t.Run(ConfigInfo(test), func(t *testing.T) {
			sc := NewServerController()
			go func(config ServerConfig, sc *ServerController) {
				_, _ = Serve(context.Background(), "", config, sc, env)
			}(test, sc)
			err := sc.WaitForStart()
			require.NoError(t, err)
			conn, err := dbr.Open("mysql", ConnectionString(test), nil)
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
	env := dtestutils.CreateEnvWithSeedData(t)
	serverConfig := DefaultServerConfig().withLogLevel(LogLevel_Fatal).WithPort(15300)

	sc := NewServerController()
	defer sc.StopServer()
	go func() {
		_, _ = Serve(context.Background(), "", serverConfig, sc, env)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	const dbName = "dolt"
	conn, err := dbr.Open("mysql", ConnectionString(serverConfig)+dbName, nil)
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

// If a port is already in use, throw error "Port XXXX already in use."
func TestServerFailsIfPortInUse(t *testing.T) {
	serverController := NewServerController()
	server := &http.Server{
		Addr:    ":15200",
		Handler: http.DefaultServeMux,
	}
	go server.ListenAndServe()
	go func() {
		startServer(context.Background(), "test", "dolt sql-server", []string{
			"-H", "localhost",
			"-P", "15200",
			"-u", "username",
			"-p", "password",
			"-t", "5",
			"-l", "info",
			"-r",
		}, dtestutils.CreateEnvWithSeedData(t), serverController)
	}()
	err := serverController.WaitForStart()
	require.Error(t, err)
	server.Close()
}

func TestServerSetDefaultBranch(t *testing.T) {
	dEnv := dtestutils.CreateEnvWithSeedData(t)
	serverConfig := DefaultServerConfig().withLogLevel(LogLevel_Fatal).WithPort(15302)

	sc := NewServerController()
	defer sc.StopServer()
	go func() {
		_, _ = Serve(context.Background(), "", serverConfig, sc, dEnv)
	}()
	err := sc.WaitForStart()
	require.NoError(t, err)

	const dbName = "dolt"

	conn, err := dbr.Open("mysql", ConnectionString(serverConfig)+dbName, nil)
	require.NoError(t, err)
	sess := conn.NewSession(nil)

	defaultBranch := env.DefaultInitBranch

	tests := []struct {
		query       *dbr.SelectStmt
		expectedRes []testBranch
	}{
		{
			query:       sess.Select("active_branch() as branch"),
			expectedRes: []testBranch{{defaultBranch}},
		},
		{
			query:       sess.SelectBySql("set GLOBAL dolt_default_branch = 'refs/heads/new'"),
			expectedRes: []testBranch{},
		},
		{
			query:       sess.Select("active_branch() as branch"),
			expectedRes: []testBranch{{defaultBranch}},
		},
		{
			query:       sess.Select("dolt_checkout('-b', 'new')"),
			expectedRes: []testBranch{{""}},
		},
		{
			query:       sess.Select("dolt_checkout('main')"),
			expectedRes: []testBranch{{""}},
		},
	}

	for _, test := range tests {
		t.Run(test.query.Query, func(t *testing.T) {
			var branch []testBranch
			_, err := test.query.LoadContext(context.Background(), &branch)
			assert.NoError(t, err)
			assert.ElementsMatch(t, branch, test.expectedRes)
		})
	}
	conn.Close()

	conn, err = dbr.Open("mysql", ConnectionString(serverConfig)+dbName, nil)
	require.NoError(t, err)
	defer conn.Close()

	sess = conn.NewSession(nil)

	tests = []struct {
		query       *dbr.SelectStmt
		expectedRes []testBranch
	}{
		{
			query:       sess.Select("active_branch() as branch"),
			expectedRes: []testBranch{{"new"}},
		},
		{
			query:       sess.SelectBySql("set GLOBAL dolt_default_branch = 'new'"),
			expectedRes: []testBranch{},
		},
	}

	defer func(sess *dbr.Session) {
		var res []struct {
			int
		}
		sess.SelectBySql("set GLOBAL dolt_default_branch = ''").LoadContext(context.Background(), &res)
	}(sess)

	for _, test := range tests {
		t.Run(test.query.Query, func(t *testing.T) {
			var branch []testBranch
			_, err := test.query.LoadContext(context.Background(), &branch)
			assert.NoError(t, err)
			assert.ElementsMatch(t, branch, test.expectedRes)
		})
	}
	conn.Close()

	conn, err = dbr.Open("mysql", ConnectionString(serverConfig)+dbName, nil)
	require.NoError(t, err)
	defer conn.Close()

	sess = conn.NewSession(nil)

	tests = []struct {
		query       *dbr.SelectStmt
		expectedRes []testBranch
	}{
		{
			query:       sess.Select("active_branch() as branch"),
			expectedRes: []testBranch{{"new"}},
		},
	}

	for _, test := range tests {
		t.Run(test.query.Query, func(t *testing.T) {
			var branch []testBranch
			_, err := test.query.LoadContext(context.Background(), &branch)
			assert.NoError(t, err)
			assert.ElementsMatch(t, branch, test.expectedRes)
		})
	}

	var res []struct {
		int
	}
	sess.SelectBySql("set GLOBAL dolt_default_branch = ''").LoadContext(context.Background(), &res)
}

func TestReadReplica(t *testing.T) {
	var err error
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("no working directory: %s", err.Error())
	}
	defer os.Chdir(cwd)

	multiSetup := testcommands.NewMultiRepoTestSetup(t.Fatal)
	defer os.RemoveAll(multiSetup.Root)

	multiSetup.NewDB("read_replica")
	multiSetup.NewRemote("remote1")
	multiSetup.PushToRemote("read_replica", "remote1", "main")
	multiSetup.CloneDB("remote1", "source_db")

	readReplicaDbName := multiSetup.DbNames[0]
	sourceDbName := multiSetup.DbNames[1]

	localCfg, ok := multiSetup.MrEnv.GetEnv(readReplicaDbName).Config.GetConfig(env.LocalConfig)
	if !ok {
		t.Fatal("local config does not exist")
	}
	config.NewPrefixConfig(localCfg, env.SqlServerGlobalsPrefix).SetStrings(map[string]string{sqle.ReadReplicaRemoteKey: "remote1", sqle.ReplicateHeadsKey: "main,feature"})
	dsess.InitPersistedSystemVars(multiSetup.MrEnv.GetEnv(readReplicaDbName))

	// start server as read replica
	sc := NewServerController()
	serverConfig := DefaultServerConfig().withLogLevel(LogLevel_Fatal).WithPort(15303)

	func() {
		os.Chdir(multiSetup.DbPaths[readReplicaDbName])
		go func() {
			_, _ = Serve(context.Background(), "", serverConfig, sc, multiSetup.MrEnv.GetEnv(readReplicaDbName))
		}()
		err = sc.WaitForStart()
		require.NoError(t, err)
	}()
	defer sc.StopServer()

	replicatedTable := "new_table"
	multiSetup.CreateTable(sourceDbName, replicatedTable)
	multiSetup.StageAll(sourceDbName)
	_ = multiSetup.CommitWithWorkingSet(sourceDbName)
	multiSetup.PushToRemote(sourceDbName, "remote1", "main")

	t.Run("read replica pulls multiple branches", func(t *testing.T) {
		conn, err := dbr.Open("mysql", ConnectionString(serverConfig)+readReplicaDbName, nil)
		defer conn.Close()
		require.NoError(t, err)
		sess := conn.NewSession(nil)

		newBranch := "feature"
		multiSetup.NewBranch(sourceDbName, newBranch)
		multiSetup.CheckoutBranch(sourceDbName, newBranch)
		multiSetup.PushToRemote(sourceDbName, "remote1", newBranch)

		var res []int

		q := sess.SelectBySql(fmt.Sprintf("select dolt_checkout('%s')", newBranch))
		_, err = q.LoadContext(context.Background(), &res)
		assert.NoError(t, err)
		assert.ElementsMatch(t, res, []int{0})
	})
}
