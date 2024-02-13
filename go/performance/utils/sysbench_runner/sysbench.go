package sysbench_runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

type sysbenchTesterImpl struct {
	test         Test
	config       Config
	serverConfig ServerConfig
	serverParams []string
	stampFunc    func() string
	idFunc       func() string
	suiteId      string
}

var _ Tester = &sysbenchTesterImpl{}

func NewSysbenchTester(config Config, serverConfig ServerConfig, test Test, serverParams []string, stampFunc func() string) *sysbenchTesterImpl {
	return &sysbenchTesterImpl{
		config:       config,
		serverParams: serverParams,
		serverConfig: serverConfig,
		test:         test,
		suiteId:      serverConfig.Id(),
		stampFunc:    stampFunc,
	}
}

func (t *sysbenchTesterImpl) newResult() (*Result, error) {
	serverParams, err := t.serverConfig.ServerArgs()
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
	if t.test.FromScript() {
		base := filepath.Base(t.test.Name())
		ext := filepath.Ext(base)
		name = strings.TrimSuffix(base, ext)
	} else {
		name = t.test.Name()
	}

	return &Result{
		Id:            getId(),
		SuiteId:       t.suiteId,
		TestId:        t.test.Id(),
		RuntimeOS:     t.config.RuntimeOs(),
		RuntimeGoArch: t.config.RuntimeGoArch(),
		ServerName:    string(t.serverConfig.ServerType()),
		ServerVersion: t.serverConfig.Version(),
		ServerParams:  strings.Join(serverParams, " "),
		TestName:      name,
		TestParams:    strings.Join(t.test.ParamsToSlice(), " "),
	}, nil
}

func (t *sysbenchTesterImpl) outputToResult(output []byte) (*Result, error) {
	return OutputToResult(output, t.serverConfig.ServerType(), t.serverConfig.Version(), t.test.Name(), t.test.Id(), t.suiteId, t.config.RuntimeOs(), t.config.RuntimeGoArch(), t.serverParams, t.test.ParamsToSlice(), nil, t.test.FromScript())
}

func (t *sysbenchTesterImpl) prepare(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, sysbenchCommand, t.test.PrepareArgs()...)
	if t.test.FromScript() {
		lp := filepath.Join(t.config.ScriptDir(), luaPath)
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

func (t *sysbenchTesterImpl) run(ctx context.Context) (*Result, error) {
	cmd := exec.CommandContext(ctx, sysbenchCommand, t.test.RunArgs()...)
	if t.test.FromScript() {
		lp := filepath.Join(t.config.ScriptDir(), luaPath)
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

func (t *sysbenchTesterImpl) cleanup(ctx context.Context) error {
	cmd := ExecCommand(ctx, sysbenchCommand, t.test.CleanupArgs()...)
	if t.test.FromScript() {
		lp := filepath.Join(t.config.ScriptDir(), luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf(luaPathEnvVarTemplate, lp))
	}
	return cmd.Run()
}

func (t *sysbenchTesterImpl) Test(ctx context.Context) (*Result, error) {
	err := t.prepare(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Running test", t.test.Name)

	rs, err := t.run(ctx)
	if err != nil {
		return nil, err
	}

	return rs, t.cleanup(ctx)
}
