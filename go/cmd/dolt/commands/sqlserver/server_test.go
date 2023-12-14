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
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
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

type testResult struct {
	Branch string
}

var (
	bill = testPerson{"Bill Billerson", 32, true, "Senior Dufus"}
	john = testPerson{"John Johnson", 25, false, "Dufus"}
	rob  = testPerson{"Rob Robertson", 21, false, ""}
)

func TestServerArgs(t *testing.T) {
	controller := svcs.NewController()
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB.Close())
	}()
	go func() {
		StartServer(context.Background(), "0.0.0", "dolt sql-server", []string{
			"-H", "localhost",
			"-P", "15200",
			"-u", "username",
			"-p", "password",
			"-t", "5",
			"-l", "info",
			"-r",
		}, dEnv, controller)
	}()
	err = controller.WaitForStart()
	require.NoError(t, err)
	conn, err := dbr.Open("mysql", "username:password@tcp(localhost:15200)/", nil)
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)
	controller.Stop()
	err = controller.WaitForStop()
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
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB.Close())
	}()
	controller := svcs.NewController()
	go func() {

		dEnv.FS.WriteFile("config.yaml", []byte(yamlConfig), os.ModePerm)
		StartServer(context.Background(), "0.0.0", "dolt sql-server", []string{
			"--config", "config.yaml",
		}, dEnv, controller)
	}()
	err = controller.WaitForStart()
	require.NoError(t, err)
	conn, err := dbr.Open("mysql", "username:password@tcp(localhost:15200)/", nil)
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)
	controller.Stop()
	err = controller.WaitForStop()
	assert.NoError(t, err)
}

func TestServerBadArgs(t *testing.T) {
	env, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, env.DoltDB.Close())
	}()

	tests := [][]string{
		{"-H", "127.0.0.0.1"},
		{"-H", "loclahost"},
		{"-P", "300"},
		{"-P", "90000"},
		{"-l", "everything"},
	}

	for _, test := range tests {
		test := test
		t.Run(strings.Join(test, " "), func(t *testing.T) {
			controller := svcs.NewController()
			go func() {
				StartServer(context.Background(), "test", "dolt sql-server", test, env, controller)
			}()
			if !assert.Error(t, controller.WaitForStart()) {
				controller.Stop()
			}
		})
	}
}

func TestServerGoodParams(t *testing.T) {
	env, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, env.DoltDB.Close())
	}()

	tests := []ServerConfig{
		DefaultServerConfig(),
		DefaultServerConfig().WithHost("127.0.0.1").WithPort(15400),
		DefaultServerConfig().WithHost("localhost").WithPort(15401),
		//DefaultServerConfig().WithHost("::1").WithPort(15402), // Fails on Jenkins, assuming no IPv6 support
		DefaultServerConfig().withUser("testusername").WithPort(15403),
		DefaultServerConfig().withPassword("hunter2").WithPort(15404),
		DefaultServerConfig().withTimeout(0).WithPort(15405),
		DefaultServerConfig().withTimeout(5).WithPort(15406),
		DefaultServerConfig().withLogLevel(LogLevel_Debug).WithPort(15407),
		DefaultServerConfig().withLogLevel(LogLevel_Info).WithPort(15408),
		DefaultServerConfig().withReadOnly(true).WithPort(15409),
		DefaultServerConfig().withUser("testusernamE").withPassword("hunter2").withTimeout(4).WithPort(15410),
		DefaultServerConfig().withAllowCleartextPasswords(true),
	}

	for _, test := range tests {
		t.Run(ConfigInfo(test), func(t *testing.T) {
			sc := svcs.NewController()
			go func(config ServerConfig, sc *svcs.Controller) {
				_, _ = Serve(context.Background(), "0.0.0", config, sc, env)
			}(test, sc)
			err := sc.WaitForStart()
			require.NoError(t, err)
			conn, err := dbr.Open("mysql", ConnectionString(test, "dbname"), nil)
			require.NoError(t, err)
			err = conn.Close()
			require.NoError(t, err)
			sc.Stop()
			err = sc.WaitForStop()
			assert.NoError(t, err)
		})
	}
}

func TestServerSelect(t *testing.T) {
	env, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, env.DoltDB.Close())
	}()

	serverConfig := DefaultServerConfig().withLogLevel(LogLevel_Fatal).WithPort(15300)

	sc := svcs.NewController()
	defer sc.Stop()
	go func() {
		_, _ = Serve(context.Background(), "0.0.0", serverConfig, sc, env)
	}()
	err = sc.WaitForStart()
	require.NoError(t, err)

	const dbName = "dolt"
	conn, err := dbr.Open("mysql", ConnectionString(serverConfig, dbName), nil)
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
	controller := svcs.NewController()
	server := &http.Server{
		Addr:    ":15200",
		Handler: http.DefaultServeMux,
	}
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB.Close())
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		server.ListenAndServe()
	}()
	go func() {
		StartServer(context.Background(), "test", "dolt sql-server", []string{
			"-H", "localhost",
			"-P", "15200",
			"-u", "username",
			"-p", "password",
			"-t", "5",
			"-l", "info",
			"-r",
		}, dEnv, controller)
	}()

	err = controller.WaitForStart()
	require.Error(t, err)
	server.Close()
	wg.Wait()
}

type defaultBranchTest struct {
	query          *dbr.SelectStmt
	expectedRes    []testResult
	expectedErrStr string
}

func TestServerSetDefaultBranch(t *testing.T) {
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB.Close())
	}()

	serverConfig := DefaultServerConfig().withLogLevel(LogLevel_Fatal).WithPort(15302)

	sc := svcs.NewController()
	defer sc.Stop()
	go func() {
		_, _ = Serve(context.Background(), "0.0.0", serverConfig, sc, dEnv)
	}()
	err = sc.WaitForStart()
	require.NoError(t, err)

	const dbName = "dolt"

	defaultBranch := env.DefaultInitBranch

	conn, err := dbr.Open("mysql", ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess := conn.NewSession(nil)

	tests := []defaultBranchTest{
		{
			query:       sess.SelectBySql("select active_branch() as branch"),
			expectedRes: []testResult{{defaultBranch}},
		},
		{
			query:       sess.SelectBySql("call dolt_checkout('-b', 'new')"),
			expectedRes: []testResult{{""}},
		},
		{
			query:       sess.SelectBySql("call dolt_checkout('-b', 'new2')"),
			expectedRes: []testResult{{""}},
		},
	}

	runDefaultBranchTests(t, tests, conn)

	conn, err = dbr.Open("mysql", ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess = conn.NewSession(nil)

	tests = []defaultBranchTest{
		{
			query:       sess.SelectBySql("select active_branch() as branch"),
			expectedRes: []testResult{{defaultBranch}},
		},
		{
			query:       sess.SelectBySql("set GLOBAL dolt_default_branch = 'refs/heads/new'"),
			expectedRes: nil,
		},
		{
			query:       sess.SelectBySql("select active_branch() as branch"),
			expectedRes: []testResult{{"main"}},
		},
		{
			query:       sess.SelectBySql("call dolt_checkout('main')"),
			expectedRes: []testResult{{""}},
		},
	}

	runDefaultBranchTests(t, tests, conn)

	conn, err = dbr.Open("mysql", ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess = conn.NewSession(nil)

	tests = []defaultBranchTest{
		{
			query:       sess.SelectBySql("select active_branch() as branch"),
			expectedRes: []testResult{{"new"}},
		},
		{
			query:       sess.SelectBySql("set GLOBAL dolt_default_branch = 'new2'"),
			expectedRes: nil,
		},
	}

	runDefaultBranchTests(t, tests, conn)

	conn, err = dbr.Open("mysql", ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess = conn.NewSession(nil)

	tests = []defaultBranchTest{
		{
			query:       sess.SelectBySql("select active_branch() as branch"),
			expectedRes: []testResult{{"new2"}},
		},
	}

	runDefaultBranchTests(t, tests, conn)

	conn, err = dbr.Open("mysql", ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess = conn.NewSession(nil)

	tests = []defaultBranchTest{
		{
			query:       sess.SelectBySql("set GLOBAL dolt_default_branch = 'doesNotExist'"),
			expectedRes: nil,
		},
	}

	runDefaultBranchTests(t, tests, conn)

	conn, err = dbr.Open("mysql", ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess = conn.NewSession(nil)

	tests = []defaultBranchTest{
		{
			query:          sess.SelectBySql("select active_branch() as branch"),
			expectedErrStr: "cannot resolve default branch head", // TODO: should be a better error message
		},
	}

	runDefaultBranchTests(t, tests, conn)
}

func runDefaultBranchTests(t *testing.T, tests []defaultBranchTest, conn *dbr.Connection) {
	for _, test := range tests {
		t.Run(test.query.Query, func(t *testing.T) {
			var branch []testResult
			_, err := test.query.LoadContext(context.Background(), &branch)
			if test.expectedErrStr != "" {
				require.Error(t, err)
				assert.Containsf(t, err.Error(), test.expectedErrStr, "expected error string not found")
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedRes, branch)
			}
		})
	}
	require.NoError(t, conn.Close())
}

func TestReadReplica(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("no working directory: %s", err.Error())
	}
	defer os.Chdir(cwd)

	ctx := context.Background()

	multiSetup := testcommands.NewMultiRepoTestSetup(t.Fatal)
	defer os.RemoveAll(multiSetup.Root)
	defer multiSetup.Close()

	multiSetup.NewDB("read_replica")
	multiSetup.NewRemote("remote1")
	multiSetup.PushToRemote("read_replica", "remote1", "main")
	multiSetup.CloneDB("remote1", "source_db")

	readReplicaDbName := multiSetup.DbNames[0]
	sourceDbName := multiSetup.DbNames[1]

	localCfg, ok := multiSetup.GetEnv(readReplicaDbName).Config.GetConfig(env.LocalConfig)
	require.True(t, ok, "local config does not exist")
	config.NewPrefixConfig(localCfg, env.SqlServerGlobalsPrefix).SetStrings(map[string]string{dsess.ReadReplicaRemote: "remote1", dsess.ReplicateHeads: "main"})
	dsess.InitPersistedSystemVars(multiSetup.GetEnv(readReplicaDbName))

	// start server as read replica
	sc := svcs.NewController()
	serverConfig := DefaultServerConfig().withLogLevel(LogLevel_Fatal).WithPort(15303)

	// set socket to nil to force tcp
	serverConfig = serverConfig.WithHost("127.0.0.1").WithSocket("")

	os.Chdir(multiSetup.DbPaths[readReplicaDbName])
	go func() {
		err, _ = Serve(context.Background(), "0.0.0", serverConfig, sc, multiSetup.GetEnv(readReplicaDbName))
		require.NoError(t, err)
	}()
	require.NoError(t, sc.WaitForStart())
	defer sc.Stop()

	replicatedTable := "new_table"
	multiSetup.CreateTable(ctx, sourceDbName, replicatedTable)
	multiSetup.StageAll(sourceDbName)
	_ = multiSetup.CommitWithWorkingSet(sourceDbName)
	multiSetup.PushToRemote(sourceDbName, "remote1", "main")

	t.Run("read replica pulls multiple branches", func(t *testing.T) {
		conn, err := dbr.Open("mysql", ConnectionString(serverConfig, readReplicaDbName), nil)
		defer conn.Close()
		require.NoError(t, err)
		sess := conn.NewSession(nil)

		multiSetup.NewBranch(sourceDbName, "feature")
		multiSetup.CheckoutBranch(sourceDbName, "feature")
		multiSetup.PushToRemote(sourceDbName, "remote1", "feature")

		// Configure the read replica to pull the new feature branch we just created
		config.NewPrefixConfig(localCfg, env.SqlServerGlobalsPrefix).SetStrings(map[string]string{dsess.ReadReplicaRemote: "remote1", dsess.ReplicateHeads: "main,feature"})
		dsess.InitPersistedSystemVars(multiSetup.GetEnv(readReplicaDbName))

		var res []int
		q := sess.SelectBySql("call dolt_checkout('feature');")
		_, err = q.LoadContext(context.Background(), &res)
		require.NoError(t, err)
		assert.ElementsMatch(t, res, []int{0})
	})
}
