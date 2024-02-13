package sysbench_runner

import (
	"context"
	"fmt"
	"os"
	"syscall"
)

type mysqlTpccBenchmarkerImpl struct {
	dir          string // cwd
	config       *TpccBenchmarkConfig
	serverConfig *doltServerConfigImpl
}

var _ Benchmarker = &mysqlTpccBenchmarkerImpl{}

func NewMysqlTpccBenchmarker(dir string, config *TpccBenchmarkConfig, serverConfig *doltServerConfigImpl) *mysqlTpccBenchmarkerImpl {
	return &mysqlTpccBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *mysqlTpccBenchmarkerImpl) getDsn() (string, error) {
	return GetMysqlDsn(b.serverConfig.Host, b.serverConfig.Socket, b.serverConfig.ConnectionProtocol, b.serverConfig.Port)
}

func (b *mysqlTpccBenchmarkerImpl) createTestingDb(ctx context.Context) error {
	dsn, err := b.getDsn()
	if err != nil {
		return err
	}
	return CreateMysqlTestingDb(ctx, dsn, tpccDbName)
}

func (b *mysqlTpccBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	serverDir, err := InitMysqlDataDir(ctx, b.serverConfig.ServerExec, tpccDbName)
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

	tests := GetTpccTests(b.config)

	results := make(Results, 0)
	for _, test := range tests {
		tester := NewTpccTester(b.config, b.serverConfig, test, serverParams, stampFunc)
		r, err := tester.Test(ctx)
		if err != nil {
			server.Stop()
			return nil, err
		}
		results = append(results, r)
	}

	err = server.Stop()
	if err != nil {
		return nil, err
	}

	return results, os.RemoveAll(serverDir)
}
