package sysbench_runner

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/dolthub/dolt/go/store/types"
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

var stampFunc = func() string { return time.Now().UTC().Format(stampFormat) }

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

func (b *doltBenchmarkerImpl) updateGlobalConfig(ctx context.Context) error {
	err := CheckSetDoltConfig(ctx, b.serverConfig.ServerExec, doltConfigUsernameKey, doltBenchmarkUser)
	if err != nil {
		return err
	}
	return CheckSetDoltConfig(ctx, b.serverConfig.ServerExec, doltConfigEmailKey, doltBenchmarkEmail)
}

func (b *doltBenchmarkerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, b.serverConfig.ServerExec, doltVersionCommand)
	return version.Run()
}

func (b *doltBenchmarkerImpl) initDoltRepo(ctx context.Context) (string, error) {
	return InitDoltRepo(ctx, b.dir, b.serverConfig.ServerExec, b.config.NomsBinFormat, dbName)
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

	tests, err := GetTests(b.config, b.serverConfig, nil)
	if err != nil {
		return nil, err
	}

	results := make(Results, 0)
	for i := 0; i < b.config.Runs; i++ {
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

	return results, nil
}

// InitDoltRepo initializes a dolt database and returns its path
func InitDoltRepo(ctx context.Context, dir, serverExec, nomsBinFormat, dbName string) (string, error) {
	testRepo := filepath.Join(dir, dbName)
	if nomsBinFormat == types.Format_LD_1.VersionString() {
		err := ExecCommand(ctx, serverExec, doltCloneCommand, bigEmptyRepo, dbName).Run()
		if err != nil {
			return "", err
		}
		return testRepo, nil
	}

	err := os.MkdirAll(testRepo, os.ModePerm)
	if err != nil {
		return "", err
	}

	if nomsBinFormat != "" {
		if err = os.Setenv(nbfEnvVar, nomsBinFormat); err != nil {
			return "", err
		}
	}

	doltInit := ExecCommand(ctx, serverExec, doltInitCommand)
	doltInit.Dir = testRepo
	err = doltInit.Run()
	if err != nil {
		return "", err
	}

	return testRepo, nil
}

// CheckSetDoltConfig checks the output of `dolt config --global --get` and sets the key, val if necessary
func CheckSetDoltConfig(ctx context.Context, serverExec, key, val string) error {
	check := ExecCommand(ctx, serverExec, doltConfigCommand, doltConfigGlobalFlag, doltConfigGetFlag, key)
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
