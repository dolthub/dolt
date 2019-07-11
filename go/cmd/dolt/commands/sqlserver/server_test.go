package sqlserver

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"strings"
	"testing"
)

type testPerson struct {
	Name string
	Age int
	Is_married bool
	Title string
}

var (
	bill = testPerson{"Bill Billerson", 32, true, "Senior Dufus"}
	john = testPerson{"John Johnson", 25, false, "Dufus"}
	rob = testPerson{"Rob Robertson", 21, false, ""}
)

func TestServerArgs(t *testing.T) {
	serverController := CreateServerController()
	go func() {
		sqlServerImpl("dolt sql-server", []string{
			"-a", "localhost",
			"-p", "15200",
			"-u", "username",
			"-w", "password",
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

	tests := [][]string {
		{"-a", "127.0.0.0.1"},
		{"-a", "loclahost"},
		{"-p", "300"},
		{"-p", "90000"},
		{"-u", ""},
		{"-t", "-1"},
		{"-l", "everything"},
	}

	for _, test := range tests {
		t.Run(strings.Join(test, " "), func(t *testing.T) {
			serverController := CreateServerController()
			go func(serverController *ServerController){
				sqlServerImpl("dolt sql-server", test, env, serverController)
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
	root, verr := commands.GetWorkingWithVErr(env)
	require.NoError(t, verr)

	tests := []*ServerConfig {
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
				serve(config, root, sc)
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
	root, verr := commands.GetWorkingWithVErr(env)
	require.NoError(t, verr)
	serverConfig := DefaultServerConfig().WithLogLevel(LogLevel_Fatal).WithPort(15300)

	sc := CreateServerController()
	defer sc.StopServer()
	go func() {
		serve(serverConfig, root, sc)
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
		{func() *dbr.SelectStmt { return sess.Select("*").From("people")}, []testPerson{bill, john, rob} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age = 32")}, []testPerson{bill} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("title = 'Senior Dufus'")}, []testPerson{bill} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name = 'Bill Billerson'")}, []testPerson{bill} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name = 'John Johnson'")}, []testPerson{john} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age = 25")}, []testPerson{john} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("25 = age")}, []testPerson{john} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("is_married = false")}, []testPerson{john, rob} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age < 30")}, []testPerson{john, rob} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age > 24")}, []testPerson{bill, john} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("age >= 25")}, []testPerson{bill, john} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name <= 'John Johnson'")}, []testPerson{bill, john} },
		{func() *dbr.SelectStmt { return sess.Select("*").From("people").Where("name <> 'John Johnson'")}, []testPerson{bill, rob} },
		{func() *dbr.SelectStmt { return sess.Select("age, is_married").From("people").Where("name = 'John Johnson'")}, []testPerson{{"", 25, false, ""}} },
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
