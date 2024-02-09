package sysbench_runner

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	sysbenchCommand       = "sysbench"
	luaPathEnvVarTemplate = "LUA_PATH=%s"
)

type SysbenchTester interface {
	Test(ctx context.Context) (*Result, error)
}

type sysbenchTestImpl struct {
	test         *Test
	config       *Config
	scriptDir    string
	serverConfig *ServerConfig
	stampFunc    func() string
	idFunc       func() string
	suiteId      string
}

var _ SysbenchTester = &sysbenchTestImpl{}

func NewSysbenchTester(config *Config, serverConfig *ServerConfig, test *Test, stampFunc func() string) *sysbenchTestImpl {
	return &sysbenchTestImpl{
		config:       config,
		serverConfig: serverConfig,
		test:         test,
		suiteId:      serverConfig.GetId(),
		stampFunc:    stampFunc,
	}
}

func (t *sysbenchTestImpl) newResult() (*Result, error) {
	serverParams, err := t.serverConfig.GetServerArgs()
	if err != nil {
		return nil, err
	}

	var getId func() string
	if t.idFunc == nil {
		getId = func() string {
			return uuid.New().String()
		}
	} else {
		getId = t.idFunc
	}

	var name string
	if t.test.FromScript {
		base := filepath.Base(t.test.Name)
		ext := filepath.Ext(base)
		name = strings.TrimSuffix(base, ext)
	} else {
		name = t.test.Name
	}

	return &Result{
		Id:            getId(),
		SuiteId:       t.suiteId,
		TestId:        t.test.id,
		RuntimeOS:     t.config.RuntimeOS,
		RuntimeGoArch: t.config.RuntimeGoArch,
		ServerName:    string(t.serverConfig.Server),
		ServerVersion: t.serverConfig.Version,
		ServerParams:  strings.Join(serverParams, " "),
		TestName:      name,
		TestParams:    strings.Join(t.test.Params, " "),
	}, nil
}

func (t *sysbenchTestImpl) updateResult(result *Result, line string) error {
	lineParts := strings.Split(line, ":")
	key := strings.TrimSpace(lineParts[0])

	if len(lineParts) > 1 {
		rawVal := strings.TrimSpace(lineParts[1])
		err := updateResult(result, key, rawVal)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *sysbenchTestImpl) outputToResult(output []byte) (*Result, error) {
	result, err := t.newResult()
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
		if strings.HasPrefix(trimmed, SqlStatsPrefix) {
			process = true
			continue
		}
		if process {
			err := t.updateResult(result, trimmed)
			if err != nil {
				return result, err
			}
		}
	}

	return result, nil
}

func (t *sysbenchTestImpl) prepare(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, sysbenchCommand, t.test.Prepare()...)
	if t.test.FromScript {
		lp := filepath.Join(t.scriptDir, luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf(luaPathEnvVarTemplate, lp))
	}
	out, err := cmd.Output()
	if err != nil {
		fmt.Println(string(out))
		return err
	}
	return nil
}

func (t *sysbenchTestImpl) run(ctx context.Context) (*Result, error) {
	cmd := exec.CommandContext(ctx, sysbenchCommand, t.test.Run()...)
	if t.test.FromScript {
		lp := filepath.Join(t.scriptDir, luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf(luaPathEnvVarTemplate, lp))
	}

	out, err := cmd.Output()
	if err != nil {
		fmt.Print(string(out))
		return nil, err
	}

	if Debug == true {
		fmt.Print(string(out))
	}

	rs, err := t.outputToResult(out)
	if err != nil {
		return nil, err
	}

	rs.Stamp(t.stampFunc)

	return rs, nil
}

func (t *sysbenchTestImpl) cleanup(ctx context.Context) error {
	cmd := ExecCommand(ctx, sysbenchCommand, t.test.Cleanup()...)
	if t.test.FromScript {
		lp := filepath.Join(t.scriptDir, luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf(luaPathEnvVarTemplate, lp))
	}
	return cmd.Run()
}

func (t *sysbenchTestImpl) Test(ctx context.Context) (*Result, error) {
	err := t.prepare(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Running test ", t.test.Name)

	rs, err := t.run(ctx)
	if err != nil {
		return nil, err
	}

	return rs, t.cleanup(ctx)
}
