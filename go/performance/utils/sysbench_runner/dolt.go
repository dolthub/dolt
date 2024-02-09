package sysbench_runner

import (
	"context"
	"github.com/dolthub/dolt/go/store/types"
	"os"
	"path/filepath"
	"syscall"
)

const (
	doltConfigUsernameKey = "user.name"
	doltConfigEmailKey    = "user.email"
	doltBenchmarkUser     = "benchmark"
	doltBenchmarkEmail    = "benchmark@dolthub.com"
	doltConfigCommand     = "config"
	doltConfigGlobalFlag  = "--global"
	doltConfigGetFlag     = "--get"
	doltConfigAddFlag     = "--add"
	doltCloneCommand      = "clone"
	doltVersionCommand    = "version"
	doltInitCommand       = "init"
	dbName                = "test"
	bigEmptyRepo          = "max-hoffman/big-empty"
	nbfEnvVar             = "DOLT_DEFAULT_BIN_FORMAT"
)

type doltBenchmarkerImpl struct {
	dir          string // cwd
	config       *Config
	serverConfig *ServerConfig
}

var _ Benchmarker = &doltBenchmarkerImpl{}

func NewDoltBenchmarker(dir string, config *Config, serverConfig *ServerConfig) *doltBenchmarkerImpl {
	return &doltBenchmarkerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

// checkSetDoltConfig checks the output of `dolt config --global --get` and sets the key, val if necessary
func (b *doltBenchmarkerImpl) checkSetDoltConfig(ctx context.Context, serverExec, key, val string) error {
	check := ExecCommand(ctx, b.serverConfig.ServerExec, doltConfigCommand, doltConfigGlobalFlag, doltConfigGetFlag, key)
	err := check.Run()
	if err != nil {
		// config get calls exit with 1 if not set
		if err.Error() != "exit status 1" {
			return err
		}
		set := ExecCommand(ctx, serverExec, doltConfigCommand, doltConfigGlobalFlag, doltConfigAddFlag, key, val)
		err := set.Run()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *doltBenchmarkerImpl) updateGlobalConfig(ctx context.Context) error {
	err := checkSetDoltConfig(ctx, b.serverConfig.ServerExec, doltConfigUsernameKey, doltBenchmarkUser)
	if err != nil {
		return err
	}
	return checkSetDoltConfig(ctx, b.serverConfig.ServerExec, doltConfigEmailKey, doltBenchmarkEmail)
}

func (b *doltBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.ServerExec, doltVersionCommand)
	return version.Run()
}

// initDoltRepo initializes a dolt repo and returns the repo path
func (b *doltBenchmarkerImpl) initDoltRepo(ctx context.Context) (string, error) {
	testRepo := filepath.Join(b.dir, dbName)
	if b.config.NomsBinFormat == types.Format_LD_1.VersionString() {
		err := ExecCommand(ctx, b.serverConfig.ServerExec, doltCloneCommand, bigEmptyRepo, dbName).Run()
		if err != nil {
			return "", err
		}
		return testRepo, nil
	}

	err := os.MkdirAll(testRepo, os.ModePerm)
	if err != nil {
		return "", err
	}

	if b.config.NomsBinFormat != "" {
		if err = os.Setenv(nbfEnvVar, b.config.NomsBinFormat); err != nil {
			return "", err
		}
	}

	doltInit := ExecCommand(ctx, b.serverConfig.ServerExec, doltInitCommand)
	doltInit.Dir = testRepo
	err = doltInit.Run()
	if err != nil {
		return "", err
	}

	return testRepo, nil
}

func (b *doltBenchmarkerImpl) Benchmark(ctx context.Context) (Results, error) {
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

	serverParams, err := b.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}

	server := NewDoltServer(ctx, testRepo, b.serverConfig, syscall.SIGTERM, serverParams)
	err = server.Start(ctx)
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
				server.Stop(ctx)
				return nil, err
			}
			results = append(results, r)
		}
	}

	err = server.Stop(ctx)
	if err != nil {
		return nil, err
	}

	return results, os.RemoveAll(testRepo)
}
