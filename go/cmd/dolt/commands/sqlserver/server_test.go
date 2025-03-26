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
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
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
	ctx := context.Background()
	controller := svcs.NewController()
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB(ctx).Close())
	}()
	go func() {
		StartServer(context.Background(), "0.0.0", "dolt sql-server", []string{
			"-H", "localhost",
			"-P", "15200",
			"-t", "5",
			"-l", "info",
			"-r",
		}, dEnv, dEnv.FS, controller)
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

func TestDeprecatedUserPasswordServerArgs(t *testing.T) {
	ctx := context.Background()
	controller := svcs.NewController()
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB(ctx).Close())
	}()
	err = StartServer(ctx, "0.0.0", "dolt sql-server", []string{
		"-H", "localhost",
		"-P", "15200",
		"-u", "username",
		"-p", "password",
		"-t", "5",
		"-l", "info",
		"-r",
	}, dEnv, dEnv.FS, controller)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--user and --password have been removed from the sql-server command.")
	require.Contains(t, err.Error(), "Create users explicitly with CREATE USER and GRANT statements instead.")
}

func TestYAMLServerArgs(t *testing.T) {
	const yamlConfig = `
log_level: info

behavior:
    read_only: true

listener:
    host: localhost
    port: 15200
    read_timeout_millis: 5000
    write_timeout_millis: 5000
`
	ctx := context.Background()
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB(ctx).Close())
	}()
	controller := svcs.NewController()
	go func() {
		dEnv.FS.WriteFile("config.yaml", []byte(yamlConfig), os.ModePerm)
		err := StartServer(context.Background(), "0.0.0", "dolt sql-server", []string{
			"--config", "config.yaml",
		}, dEnv, dEnv.FS, controller)
		require.NoError(t, err)
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
	ctx := context.Background()
	env, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, env.DoltDB(ctx).Close())
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
				StartServer(context.Background(), "test", "dolt sql-server", test, env, env.FS, controller)
			}()
			if !assert.Error(t, controller.WaitForStart()) {
				controller.Stop()
			}
		})
	}
}

func TestServerGoodParams(t *testing.T) {
	ctx := context.Background()

	tests := []servercfg.ServerConfig{
		DefaultCommandLineServerConfig(),
		DefaultCommandLineServerConfig().WithHost("127.0.0.1").WithPort(15400),
		DefaultCommandLineServerConfig().WithHost("localhost").WithPort(15401),
		//DefaultCommandLineServerConfig().WithHost("::1").WithPort(15402), // Fails on Jenkins, assuming no IPv6 support
		DefaultCommandLineServerConfig().withUser("testusername").WithPort(15403),
		DefaultCommandLineServerConfig().withPassword("hunter2").WithPort(15404),
		DefaultCommandLineServerConfig().withTimeout(0).WithPort(15405),
		DefaultCommandLineServerConfig().withTimeout(5).WithPort(15406),
		DefaultCommandLineServerConfig().withLogLevel(servercfg.LogLevel_Debug).WithPort(15407),
		DefaultCommandLineServerConfig().withLogLevel(servercfg.LogLevel_Info).WithPort(15408),
		DefaultCommandLineServerConfig().withReadOnly(true).WithPort(15409),
		DefaultCommandLineServerConfig().withUser("testusernamE").withPassword("hunter2").withTimeout(4).WithPort(15410),
		DefaultCommandLineServerConfig().withLogFormat(servercfg.LogFormat_Text).WithPort(15411),
		DefaultCommandLineServerConfig().withLogFormat(servercfg.LogFormat_JSON).WithPort(15412),
		DefaultCommandLineServerConfig().withAllowCleartextPasswords(true),
	}

	for _, test := range tests {
		t.Run(servercfg.ConfigInfo(test), func(t *testing.T) {
			env, err := sqle.CreateEnvWithSeedData()
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, env.DoltDB(ctx).Close())
			}()
			sc := svcs.NewController()
			go func(config servercfg.ServerConfig, sc *svcs.Controller) {
				_, _ = Serve(context.Background(), &Config{
					Version:      "0.0.0",
					ServerConfig: config,
					Controller:   sc,
					DoltEnv:      env,
				})
			}(test, sc)
			err = sc.WaitForStart()
			require.NoError(t, err)
			conn, err := dbr.Open("mysql", servercfg.ConnectionString(test, "dbname"), nil)
			require.NoError(t, err)
			err = conn.Close()
			require.NoError(t, err)
			sc.Stop()
			err = sc.WaitForStop()
			assert.NoError(t, err)
			fmt.Println("stop server")
		})
	}
}

func TestServerSelect(t *testing.T) {
	ctx := context.Background()
	env, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, env.DoltDB(ctx).Close())
	}()

	serverConfig := DefaultCommandLineServerConfig().withLogLevel(servercfg.LogLevel_Fatal).WithPort(15300)

	sc := svcs.NewController()
	defer sc.Stop()
	go func() {
		_, _ = Serve(context.Background(), &Config{
			Version:      "0.0.0",
			ServerConfig: serverConfig,
			Controller:   sc,
			DoltEnv:      env,
		})
	}()
	err = sc.WaitForStart()
	require.NoError(t, err)

	const dbName = "dolt"
	conn, err := dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
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
	ctx := context.Background()
	controller := svcs.NewController()
	server := &http.Server{
		Addr:    ":15200",
		Handler: http.DefaultServeMux,
	}
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB(ctx).Close())
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
			"-t", "5",
			"-l", "info",
			"-r",
		}, dEnv, dEnv.FS, controller)
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
	ctx := context.Background()
	dEnv, err := sqle.CreateEnvWithSeedData()
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dEnv.DoltDB(ctx).Close())
	}()

	serverConfig := DefaultCommandLineServerConfig().withLogLevel(servercfg.LogLevel_Fatal).WithPort(15302)

	sc := svcs.NewController()
	defer sc.Stop()
	go func() {
		_, _ = Serve(context.Background(), &Config{
			Version:      "0.0.0",
			ServerConfig: serverConfig,
			Controller:   sc,
			DoltEnv:      dEnv,
		})
	}()
	err = sc.WaitForStart()
	require.NoError(t, err)

	const dbName = "dolt"

	defaultBranch := env.DefaultInitBranch

	conn, err := dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
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

	conn, err = dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
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

	conn, err = dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
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

	conn, err = dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess = conn.NewSession(nil)

	tests = []defaultBranchTest{
		{
			query:       sess.SelectBySql("select active_branch() as branch"),
			expectedRes: []testResult{{"new2"}},
		},
	}

	runDefaultBranchTests(t, tests, conn)

	conn, err = dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
	require.NoError(t, err)
	sess = conn.NewSession(nil)

	tests = []defaultBranchTest{
		{
			query:       sess.SelectBySql("set GLOBAL dolt_default_branch = 'doesNotExist'"),
			expectedRes: nil,
		},
	}

	runDefaultBranchTests(t, tests, conn)

	conn, err = dbr.Open("mysql", servercfg.ConnectionString(serverConfig, dbName), nil)
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
	serverConfig := DefaultCommandLineServerConfig().withLogLevel(servercfg.LogLevel_Fatal).WithPort(15303)

	// set socket to nil to force tcp
	serverConfig = serverConfig.WithHost("127.0.0.1").WithSocket("")

	os.Chdir(multiSetup.DbPaths[readReplicaDbName])
	go func() {
		err, _ = Serve(context.Background(), &Config{
			Version:      "0.0.0",
			ServerConfig: serverConfig,
			Controller:   sc,
			DoltEnv:      multiSetup.GetEnv(readReplicaDbName),
		})
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
		conn, err := dbr.Open("mysql", servercfg.ConnectionString(serverConfig, readReplicaDbName), nil)
		defer conn.Close()
		require.NoError(t, err)
		sess := conn.NewSession(nil)

		multiSetup.NewBranch(ctx, sourceDbName, "feature")
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

func TestGenerateYamlConfig(t *testing.T) {
	args := []string{
		"--timeout", "11",
		"--branch-control-file", "dir1/dir2/abc.db",
	}

	privilegeFilePath, err := filepath.Localize(".doltcfg/privileges.db")
	require.NoError(t, err)

	expected := `# Dolt SQL server configuration
#
# Uncomment and edit lines as necessary to modify your configuration.
# Full documentation: https://docs.dolthub.com/sql-reference/server/configuration
#

# log_level: info

# log_format: text

# max_logged_query_len: 0

# encode_logged_query: false

# behavior:
  # read_only: false
  # autocommit: true
  # disable_client_multi_statements: false
  # dolt_transaction_commit: false
  # event_scheduler: "OFF"
  # auto_gc_behavior:
    # enable: false

listener:
  # host: localhost
  # port: 3306
  # max_connections: 1000
  # back_log: 50
  # max_connections_timeout_millis: 60000
  read_timeout_millis: 11000
  write_timeout_millis: 11000
  # tls_key: key.pem
  # tls_cert: cert.pem
  # require_secure_transport: false
  # allow_cleartext_passwords: false
  # socket: /tmp/mysql.sock

# data_dir: .

# cfg_dir: .doltcfg

# remotesapi:
  # port: 8000
  # read_only: false

# privilege_file: ` + privilegeFilePath +
		`

branch_control_file: dir1/dir2/abc.db

# user_session_vars:
# - name: root
  # vars:
    # dolt_log_level: warn
    # dolt_show_system_tables: 1

# system_variables:
  # dolt_log_level: info
  # dolt_transaction_commit: 1

# jwks: []

# metrics:
  # labels: {}
  # host: localhost
  # port: 9091

# cluster:
  # standby_remotes:
  # - name: standby_replica_one
    # remote_url_template: https://standby_replica_one.svc.cluster.local:50051/{database}
  # - name: standby_replica_two
    # remote_url_template: https://standby_replica_two.svc.cluster.local:50051/{database}
  # bootstrap_role: primary
  # bootstrap_epoch: 1
  # remotesapi:
    # address: 127.0.0.1
    # port: 50051
    # tls_key: remotesapi_key.pem
    # tls_cert: remotesapi_chain.pem
    # tls_ca: standby_cas.pem
    # server_name_urls:
    # - https://standby_replica_one.svc.cluster.local
    # - https://standby_replica_two.svc.cluster.local
    # server_name_dns:
    # - standby_replica_one.svc.cluster.local
    # - standby_replica_two.svc.cluster.local`

	ap := SqlServerCmd{}.ArgParser()

	dEnv := sqle.CreateTestEnv()

	cwd, err := os.Getwd()
	require.NoError(t, err)
	cwdFs, err := filesys.LocalFilesysWithWorkingDir(cwd)
	require.NoError(t, err)

	apr := cli.ParseArgsOrDie(ap, args, nil)
	serverConfig, err := ServerConfigFromArgs(apr, dEnv, cwdFs)
	require.NoError(t, err)

	assert.Equal(t, expected, generateYamlConfig(serverConfig))
}
