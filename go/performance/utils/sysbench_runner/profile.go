package sysbench_runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type Profiler interface {
	Profile(ctx context.Context) error
}

type doltProfilerImpl struct {
	dir          string // cwd
	config       *Config
	serverConfig *ServerConfig
}

var _ Profiler = &doltProfilerImpl{}

func NewDoltProfiler(dir string, config *Config, serverConfig *ServerConfig) *doltProfilerImpl {
	return &doltProfilerImpl{
		dir:          dir,
		config:       config,
		serverConfig: serverConfig,
	}
}

func (p *doltProfilerImpl) updateGlobalConfig(ctx context.Context) error {
	err := CheckSetDoltConfig(ctx, p.serverConfig.ServerExec, doltConfigUsernameKey, doltBenchmarkUser)
	if err != nil {
		return err
	}
	return CheckSetDoltConfig(ctx, p.serverConfig.ServerExec, doltConfigEmailKey, doltBenchmarkEmail)
}

func (p *doltProfilerImpl) checkInstallation(ctx context.Context) error {
	version := ExecCommand(ctx, p.serverConfig.ServerExec, doltVersionCommand)
	return version.Run()
}

func (p *doltProfilerImpl) initDoltRepo(ctx context.Context) (string, error) {
	return InitDoltRepo(ctx, p.dir, p.serverConfig.ServerExec, p.config.NomsBinFormat)
}

func (p *doltProfilerImpl) Profile(ctx context.Context) error {
	err := p.checkInstallation(ctx)
	if err != nil {
		return err
	}

	err = p.updateGlobalConfig(ctx)
	if err != nil {
		return err
	}

	testRepo, err := p.initDoltRepo(ctx)
	if err != nil {
		return err
	}
	defer os.RemoveAll(testRepo)

	serverParams, err := p.serverConfig.GetServerArgs()
	if err != nil {
		return err
	}

	profilePath, err := os.MkdirTemp("", "dolt_profile_path_*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(profilePath)

	tempProfile := filepath.Join(profilePath, cpuProfileFilename)
	profileParams := make([]string, 0)
	profileParams = append(profileParams, profileFlag, cpuProfile, profilePathFlag, profilePath)
	profileParams = append(profileParams, serverParams...)

	server := NewServer(ctx, testRepo, p.serverConfig, syscall.SIGTERM, profileParams)
	err = server.Start()
	if err != nil {
		return err
	}

	tests, err := GetTests(p.config, p.serverConfig, nil)
	if err != nil {
		return err
	}

	results := make(Results, 0)
	for i := 0; i < p.config.Runs; i++ {
		for _, test := range tests {
			tester := NewSysbenchTester(p.config, p.serverConfig, test, stampFunc)
			r, err := tester.Test(ctx)
			if err != nil {
				server.Stop()
				return err
			}
			results = append(results, r)
		}
	}

	err = server.Stop()
	if err != nil {
		return err
	}

	info, err := os.Stat(tempProfile)
	if err != nil {
		return err
	}

	if info.Size() < 1 {
		return fmt.Errorf("failed to create profile: file was empty")
	}

	finalProfile := filepath.Join(p.serverConfig.ProfilePath, fmt.Sprintf("%s_%s", p.serverConfig.Id, cpuProfileFilename))
	return os.Rename(tempProfile, finalProfile)
}
