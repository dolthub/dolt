package sysbench_runner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"syscall"

	_ "github.com/lib/pq"
)

const (
	postgresInitDbDataDirFlag         = "--pgdata"
	postgresUsernameFlag              = "--username"
	postgresUsername                  = "postgres"
	postgresDataDirFlag               = "-D"
	postgresDropDatabaseSqlTemplate   = "DROP DATABASE IF EXISTS %s;"
	postgresDropUserSqlTemplate       = "DROP USER IF EXISTS %s;"
	postgresCreateUserSqlTemplate     = "CREATE USER %s WITH PASSWORD '%s';"
	postgresCreateDatabaseSqlTemplate = "CREATE DATABASE %s WITH OWNER %s;"
	postgresLcAllEnvVarKey            = "LC_ALL"
	postgresLcAllEnvVarValue          = "C"
)

type postgresBenchmarkerImpl struct {
	dir          string // cwd
	config       *sysbenchRunnerConfigImpl
	serverConfig *doltServerConfigImpl
}

var _ Benchmarker = &postgresBenchmarkerImpl{}

func NewPostgresBenchmarker(dir string, config *sysbenchRunnerConfigImpl, serverConfig *doltServerConfigImpl) *postgresBenchmarkerImpl {
	return &postgresBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *postgresBenchmarkerImpl) initDataDir(ctx context.Context) (string, error) {
	serverDir, err := CreateServerDir(dbName)
	if err != nil {
		return "", err
	}

	pgInit := ExecCommand(ctx, b.serverConfig.InitExec, fmt.Sprintf("%s=%s", postgresInitDbDataDirFlag, serverDir), fmt.Sprintf("%s=%s", postgresUsernameFlag, postgresUsername))
	err = pgInit.Run()
	if err != nil {
		return "", err
	}

	return serverDir, nil
}

func (b *postgresBenchmarkerImpl) createTestingDb(ctx context.Context) (err error) {
	psqlconn := fmt.Sprintf(psqlDsnTemplate, b.serverConfig.Host, b.serverConfig.Port, postgresUsername, "", dbName)

	var db *sql.DB
	db, err = sql.Open(postgresDriver, psqlconn)
	if err != nil {
		return
	}
	defer func() {
		rerr := db.Close()
		if err == nil {
			err = rerr
		}
	}()
	err = db.PingContext(ctx)
	if err != nil {
		return
	}

	stmts := []string{
		fmt.Sprintf(postgresDropDatabaseSqlTemplate, dbName),
		fmt.Sprintf(postgresDropUserSqlTemplate, sysbenchUsername),
		fmt.Sprintf(postgresCreateUserSqlTemplate, sysbenchUsername, sysbenchPassLocal),
		fmt.Sprintf(postgresCreateDatabaseSqlTemplate, dbName, sysbenchUsername),
	}

	for _, s := range stmts {
		_, err = db.ExecContext(ctx, s)
		if err != nil {
			return
		}
	}

	return
}

func (b *postgresBenchmarkerImpl) Benchmark(ctx context.Context) (results Results, err error) {
	var serverDir string
	serverDir, err = b.initDataDir(ctx)
	if err != nil {
		return
	}
	defer func() {
		rerr := os.RemoveAll(serverDir)
		if err == nil {
			err = rerr
		}
	}()

	var serverParams []string
	serverParams, err = b.serverConfig.GetServerArgs()
	if err != nil {
		return
	}

	serverParams = append(serverParams, postgresDataDirFlag, serverDir)

	server := NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
	server.WithEnv(postgresLcAllEnvVarKey, postgresLcAllEnvVarValue)

	err = server.Start()
	if err != nil {
		return
	}

	err = b.createTestingDb(ctx)
	if err != nil {
		return
	}

	var tests []*sysbenchTestImpl
	tests, err = GetTests(b.config, b.serverConfig, nil)
	if err != nil {
		return
	}

	results = make(Results, 0)
	for i := 0; i < b.config.Runs; i++ {
		for _, test := range tests {
			tester := NewSysbenchTester(b.config, b.serverConfig, test, serverParams, stampFunc)
			var r *Result
			r, err = tester.Test(ctx)
			if err != nil {
				server.Stop()
				return
			}
			results = append(results, r)
		}
	}

	err = server.Stop()
	if err != nil {
		return
	}

	return
}
