package sysbench_runner

import (
	"context"
	"fmt"
	"os"
	"syscall"
)

const (
	tpccDbName              = "sbt"
	tpccScaleFactorTemplate = "tpcc-scale-factor-%d"
)

type doltTpccBenchmarkerImpl struct {
	dir          string // cwd
	config       *TpccBenchmarkConfig
	serverConfig *ServerConfig
}

var _ Benchmarker = &doltTpccBenchmarkerImpl{}

func NewDoltTpccBenchmarker(dir string, config *TpccBenchmarkConfig, serverConfig *ServerConfig) *doltTpccBenchmarkerImpl {
	return &doltTpccBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (b *doltTpccBenchmarkerImpl) updateGlobalConfig(ctx context.Context) error {
	err := CheckSetDoltConfig(ctx, b.serverConfig.ServerExec, doltConfigUsernameKey, doltBenchmarkUser)
	if err != nil {
		return err
	}
	return CheckSetDoltConfig(ctx, b.serverConfig.ServerExec, doltConfigEmailKey, doltBenchmarkEmail)
}

func (b *doltTpccBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.ServerExec, doltVersionCommand)
	return version.Run()
}

func (b *doltTpccBenchmarkerImpl) initDoltRepo(ctx context.Context) (string, error) {
	return InitDoltRepo(ctx, b.dir, b.serverConfig.ServerExec, b.config.NomsBinFormat, tpccDbName)
}

func (b *doltTpccBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
	err := b.checkInstallation(ctx)
	if err != nil {
		return nil, err
	}

	err = b.updateGlobalConfig(ctx)
	if err != nil {
		return nil, err
	}

	testRepo, err := b.initDoltRepo(ctx)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(testRepo)

	serverParams, err := b.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}

	server := NewServer(ctx, testRepo, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start()
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

	return results, nil
}

// GetTpccTests creates a set of tests that the server needs to be executed on.
func GetTpccTests(config *TpccBenchmarkConfig) []*TpccTest {
	tests := make([]*TpccTest, 0)
	for _, sf := range config.ScaleFactors {
		params := NewDefaultTpccParams()
		params.ScaleFactor = sf
		test := NewTpccTest(fmt.Sprintf(tpccScaleFactorTemplate, sf), params)
		tests = append(tests, test)
	}
	return tests
}
