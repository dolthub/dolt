package sysbench_runner

import (
	"context"
	"fmt"
)

type tpccTesterImpl struct {
	test         Test
	config       Config
	serverConfig ServerConfig
	serverParams []string
	stampFunc    func() string
	idFunc       func() string
	suiteId      string
}

var _ Tester = &tpccTesterImpl{}

func NewTpccTester(config Config, serverConfig ServerConfig, test Test, serverParams []string, stampFunc func() string) *tpccTesterImpl {
	return &tpccTesterImpl{
		config:       config,
		serverParams: serverParams,
		serverConfig: serverConfig,
		test:         test,
		suiteId:      serverConfig.GetId(),
		stampFunc:    stampFunc,
	}
}

func (t *tpccTesterImpl) outputToResult(output []byte) (*Result, error) {
	return OutputToResult(output, t.serverConfig.GetServerType(), t.serverConfig.GetVersion(), t.test.GetName(), t.test.GetId(), t.suiteId, t.config.GetRuntimeOs(), t.config.GetRuntimeGoArch(), t.serverParams, t.test.GetParamsToSlice(), nil, false)
}

func (t *tpccTesterImpl) prepare(ctx context.Context) error {
	cmd := t.test.TpccPrepare(ctx, t.serverConfig, t.config.GetScriptDir())
	out, err := cmd.Output()
	if err != nil {
		fmt.Println(string(out))
		return err
	}
	return nil
}

func (t *tpccTesterImpl) run(ctx context.Context) (*Result, error) {
	cmd := t.test.TpccRun(ctx, t.serverConfig, t.config.GetScriptDir())
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

func (t *tpccTesterImpl) cleanup(ctx context.Context) error {
	cmd := t.test.TpccCleanup(ctx, t.serverConfig, t.config.GetScriptDir())
	return cmd.Run()
}

func (t *tpccTesterImpl) Test(ctx context.Context) (*Result, error) {
	err := t.prepare(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Running test", t.test.GetName())

	rs, err := t.run(ctx)
	if err != nil {
		return nil, err
	}

	return rs, t.cleanup(ctx)
}
