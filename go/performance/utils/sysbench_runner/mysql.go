package sysbench_runner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlDriverName                  = "mysql"
	mysqlRootTCPDsnTemplate          = "root@tcp(%s:%d)/"
	mysqlRootUnixDsnTemplate         = "root@unix(%s)/"
	mysqlDropDatabaseSqlTemplate     = "DROP DATABASE IF EXISTS %s;"
	mysqlCreateDatabaseSqlTemplate   = "CREATE DATABASE %s;"
	mysqlDropUserSqlTemplate         = "DROP USER IF EXISTS %s;"
	mysqlCreateUserSqlTemplate       = "CREATE USER %s IDENTIFIED WITH mysql_native_password BY '%s';"
	mysqlGrantPermissionsSqlTemplate = "GRANT ALL ON %s.* to %s;"
	mysqlSetGlobalLocalInfileSql     = "SET GLOBAL local_infile = 'ON';"
	mysqlSetGlobalSqlModeSql         = "SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
)

type mysqlBenchmarkerImpl struct {
	dir          string // cwd
	config       *Config
	serverConfig *ServerConfig
}

var _ Benchmarker = &mysqlBenchmarkerImpl{}

func NewMysqlBenchmarker(dir string, config *Config, serverConfig *ServerConfig) *mysqlBenchmarkerImpl {
	return &mysqlBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *mysqlBenchmarkerImpl) createServerDir() (string, error) {
	return CreateServerDir(dbName)
}

func (b *mysqlBenchmarkerImpl) initMysqlDataDir(ctx context.Context) (string, error) {
	serverDir, err := b.createServerDir()
	if err != nil {
		return "", err
	}

	msInit := ExecCommand(ctx, b.serverConfig.ServerExec, MysqlInitializeInsecureFlag, fmt.Sprintf("%s=%s", MysqlDataDirFlag, serverDir))
	err = msInit.Run()
	if err != nil {
		return "", err
	}

	return serverDir, nil
}

func (b *mysqlBenchmarkerImpl) getDsn() (string, error) {
	var socketPath string
	if b.serverConfig.Socket != "" {
		socketPath = b.serverConfig.Socket
	} else {
		socketPath = defaultMysqlSocket
	}

	if b.serverConfig.ConnectionProtocol == tcpProtocol {
		return fmt.Sprintf(mysqlRootTCPDsnTemplate, b.serverConfig.Host, b.serverConfig.Port), nil
	} else if b.serverConfig.ConnectionProtocol == unixProtocol {
		return fmt.Sprintf(mysqlRootUnixDsnTemplate, socketPath), nil
	} else {
		return "", ErrUnsupportedConnectionProtocol
	}
}

func (b *mysqlBenchmarkerImpl) createTestingDb(ctx context.Context) (err error) {
	var dsn string
	dsn, err = b.getDsn()
	if err != nil {
		return
	}

	var db *sql.DB
	db, err = sql.Open(mysqlDriverName, dsn)
	if err != nil {
		return
	}
	defer func() {
		rerr := db.Close()
		if err == nil {
			err = rerr
		}
	}()

	err = db.Ping()
	if err != nil {
		return
	}

	stmts := []string{
		fmt.Sprintf(mysqlDropDatabaseSqlTemplate, dbName),
		fmt.Sprintf(mysqlCreateDatabaseSqlTemplate, dbName),
		fmt.Sprintf(mysqlDropUserSqlTemplate, sysbenchUserLocal),
		fmt.Sprintf(mysqlCreateUserSqlTemplate, sysbenchUserLocal, sysbenchPassLocal),
		fmt.Sprintf(mysqlGrantPermissionsSqlTemplate, dbName, sysbenchUserLocal),
		mysqlSetGlobalLocalInfileSql,
		mysqlSetGlobalSqlModeSql, // Required for running groupby_scan.lua without error
	}

	for _, s := range stmts {
		_, err = db.ExecContext(ctx, s)
		if err != nil {
			return
		}
	}

	return
}

func (b *mysqlBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	serverDir, err := b.initMysqlDataDir(ctx)
	if err != nil {
		return nil, err
	}

	serverParams, err := b.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}
	serverParams = append(serverParams, fmt.Sprintf("%s=%s", MysqlDataDirFlag, serverDir))

	server := NewServer(ctx, serverDir, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start()
	if err != nil {
		return nil, err
	}

	err = b.createTestingDb(ctx)
	if err != nil {
		return nil, err
	}

	tests, err := GetTests(b.config, b.serverConfig, nil)
	if err != nil {
		return nil, err
	}

	results := make(Results, 0)
	for i := 0; i < b.config.Runs; i++ {
		for _, test := range tests {
			tester := NewSysbenchTester(b.config, b.serverConfig, test, stampFunc)
			r, err := tester.Test(ctx)
			if err != nil {
				server.Stop()
				return nil, err
			}
			results = append(results, r)
		}
	}

	err = server.Stop()
	if err != nil {
		return nil, err
	}

	return results, os.RemoveAll(serverDir)
}
