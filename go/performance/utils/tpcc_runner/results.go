package tpcc_runner

import (
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
)

// FromConfigsNewResult returns a new result with some fields set based on the provided configs
func FromConfigsNewResult(config *TpccBenchmarkConfig, serverConfig *sysbench_runner.ServerConfig, test *TpccTest, suiteId string, idFunc func() string) (*sysbench_runner.Result, error) {
	serverParams := serverConfig.GetServerArgs()

	var getId func() string
	if idFunc == nil {
		getId = func() string {
			return uuid.New().String()
		}
	} else {
		getId = idFunc
	}

	var name string
	base := filepath.Base(test.Name)
	ext := filepath.Ext(base)
	name = strings.TrimSuffix(base, ext)

	return &sysbench_runner.Result{
		Id:            getId(),
		SuiteId:       suiteId,
		TestId:        test.Id,
		RuntimeOS:     config.RuntimeOS,
		RuntimeGoArch: config.RuntimeGoArch,
		ServerName:    string(serverConfig.Server),
		ServerVersion: serverConfig.Version,
		ServerParams:  strings.Join(serverParams, " "),
		TestName:      name,
		TestParams:    strings.Join(test.getArgs(serverConfig), " "),
	}, nil
}

// FromOutputResult accepts raw sysbench run output and returns the Result
func FromOutputResult(output []byte, config *TpccBenchmarkConfig, serverConfig *sysbench_runner.ServerConfig, test *TpccTest, suiteId string, idFunc func() string) (*sysbench_runner.Result, error) {
	result, err := FromConfigsNewResult(config, serverConfig, test, suiteId, idFunc)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(output), "\n")
	var process bool
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, sysbench_runner.SqlStatsPrefix) {
			process = true
			continue
		}
		if process {
			err := sysbench_runner.UpdateResult(result, trimmed)
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}
