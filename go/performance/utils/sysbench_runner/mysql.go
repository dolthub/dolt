package sysbench_runner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
)

type mysqlBenchmarkerImpl struct {
	dir          string // cwd
	config       SysbenchConfig
	serverConfig ProtocolServerConfig
}

var _ Benchmarker = &mysqlBenchmarkerImpl{}

func NewMysqlBenchmarker(dir string, config SysbenchConfig, serverConfig ProtocolServerConfig) *mysqlBenchmarkerImpl {
	return &mysqlBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *mysqlBenchmarkerImpl) getDsn() (string, error) {
	return GetMysqlDsn(b.serverConfig.GetHost(), b.serverConfig.GetSocket(), b.serverConfig.GetConnectionProtocol(), b.serverConfig.GetPort())
}

func (b *mysqlBenchmarkerImpl) createTestingDb(ctx context.Context) error {
	dsn, err := b.getDsn()
	if err != nil {
		return err
	}
	return CreateMysqlTestingDb(ctx, dsn, dbName)
}

func (b *mysqlBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	serverDir, err := InitMysqlDataDir(ctx, b.serverConfig.GetServerExec(), dbName)
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

	tests, err := GetTests(b.config, b.serverConfig)
	if err != nil {
		return nil, err
	}

	results := make(Results, 0)
	runs := b.config.GetRuns()
	for i := 0; i < runs; i++ {
		for _, test := range tests {
			tester := NewSysbenchTester(b.config, b.serverConfig, test, serverParams, stampFunc)
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

func InitMysqlDataDir(ctx context.Context, serverExec, dbName string) (string, error) {
	serverDir, err := CreateServerDir(dbName)
	if err != nil {
		return "", err
	}

	msInit := ExecCommand(ctx, serverExec, MysqlInitializeInsecureFlag, fmt.Sprintf("%s=%s", MysqlDataDirFlag, serverDir))
	err = msInit.Run()
	if err != nil {
		return "", err
	}

	return serverDir, nil
}

func CreateMysqlTestingDb(ctx context.Context, dsn, dbName string) (err error) {
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

func GetMysqlDsn(host, socket, protocol string, port int) (string, error) {
	var socketPath string
	if socket != "" {
		socketPath = socket
	} else {
		socketPath = defaultMysqlSocket
	}

	if protocol == tcpProtocol {
		return fmt.Sprintf(mysqlRootTCPDsnTemplate, host, port), nil
	} else if protocol == unixProtocol {
		return fmt.Sprintf(mysqlRootUnixDsnTemplate, socketPath), nil
	} else {
		return "", ErrUnsupportedConnectionProtocol
	}
}
