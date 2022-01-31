package tpcc_runner

import (
	"github.com/dolthub/dolt/go/performance/utils/sysbench_runner"
	"github.com/google/uuid"
	"path/filepath"
	"strings"
)

// FromConfigsNewResult returns a new result with some fields set based on the provided configs
func FromConfigsNewResult(t *TpccTest, suiteId string, idFunc func() string) (*sysbench_runner.Result, error) {
	serverParams := t.ServerConfig.GetServerArgs()

	var getId func() string
	if idFunc == nil {
		getId = func() string {
			return uuid.New().String()
		}
	} else {
		getId = idFunc
	}

	var name string
	if t.FromScript {
		base := filepath.Base(t.Name)
		ext := filepath.Ext(base)
		name = strings.TrimSuffix(base, ext)
	} else {
		name = t.Name
	}

	return &sysbench_runner.Result{
		Id:            getId(),
		SuiteId:       suiteId,
		TestId:        t.Id,
		RuntimeOS:     "os",
		RuntimeGoArch: "goarch",
		ServerName:    string(t.ServerConfig.Server),
		ServerVersion: t.ServerConfig.Version,
		ServerParams:  strings.Join(serverParams, " "),
		TestName:      name,
		TestParams:    strings.Join(t.getArgs(), " "),
	}, nil
}

// FromOutputResult accepts raw sysbench run output and returns the Result
func FromOutputResult(output []byte, test *TpccTest, suiteId string, idFunc func() string) (*sysbench_runner.Result, error) {
	result, err := FromConfigsNewResult(test, suiteId, idFunc)
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
