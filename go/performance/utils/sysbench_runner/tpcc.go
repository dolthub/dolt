package sysbench_runner

import (
	"context"
	"fmt"
)

type tpccTestImpl struct {
	test         *TpccTest
	config       *TpccBenchmarkConfig
	serverConfig *ServerConfig
	serverParams []string
	stampFunc    func() string
	idFunc       func() string
	suiteId      string
}

var _ Tester = &tpccTestImpl{}

func NewTpccTester(config *TpccBenchmarkConfig, serverConfig *ServerConfig, test *TpccTest, serverParams []string, stampFunc func() string) *tpccTestImpl {
	return &tpccTestImpl{
		config:       config,
		serverParams: serverParams,
		serverConfig: serverConfig,
		test:         test,
		suiteId:      serverConfig.GetId(),
		stampFunc:    stampFunc,
	}
}

func (t *tpccTestImpl) outputToResult(output []byte) (*Result, error) {
	return OutputToResult(output, t.serverConfig.Server, t.serverConfig.Version, t.test.Name, t.test.Id, t.suiteId, t.config.RuntimeOS, t.config.RuntimeGoArch, t.serverParams, t.test.Params.ToSlice(), nil, false)
}

func (t *tpccTestImpl) prepare(ctx context.Context) error {
	cmd := t.test.TpccPrepare(ctx, t.serverConfig, t.config.ScriptDir)
	out, err := cmd.Output()
	if err != nil {
		fmt.Println(string(out))
		return err
	}
	return nil
}

func (t *tpccTestImpl) run(ctx context.Context) (*Result, error) {
	cmd := t.test.TpccRun(ctx, t.serverConfig, t.config.ScriptDir)
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

func (t *tpccTestImpl) cleanup(ctx context.Context) error {
	cmd := t.test.TpccCleanup(ctx, t.serverConfig, t.config.ScriptDir)
	return cmd.Run()
}

func (t *tpccTestImpl) Test(ctx context.Context) (*Result, error) {
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
