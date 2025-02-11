// Copyright 2019-2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark_runner

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type sysbenchTesterImpl struct {
	test         SysbenchTest
	config       Config
	serverConfig ServerConfig
	serverParams []string
	stampFunc    func() string
	idFunc       func() string
	suiteId      string
}

var _ Tester = &sysbenchTesterImpl{}

func NewSysbenchTester(config Config, serverConfig ServerConfig, test SysbenchTest, serverParams []string, stampFunc func() string) *sysbenchTesterImpl {
	return &sysbenchTesterImpl{
		config:       config,
		serverParams: serverParams,
		serverConfig: serverConfig,
		test:         test,
		suiteId:      serverConfig.GetId(),
		stampFunc:    stampFunc,
	}
}

func (t *sysbenchTesterImpl) newResult() (*Result, error) {
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
	if t.test.GetFromScript() {
		base := filepath.Base(t.test.GetName())
		ext := filepath.Ext(base)
		name = strings.TrimSuffix(base, ext)
	} else {
		name = t.test.GetName()
	}

	return &Result{
		Id:            getId(),
		SuiteId:       t.suiteId,
		TestId:        t.test.GetId(),
		RuntimeOS:     t.config.GetRuntimeOs(),
		RuntimeGoArch: t.config.GetRuntimeGoArch(),
		ServerName:    string(t.serverConfig.GetServerType()),
		ServerVersion: t.serverConfig.GetVersion(),
		ServerParams:  strings.Join(serverParams, " "),
		TestName:      name,
		TestParams:    strings.Join(t.test.GetParamsToSlice(), " "),
	}, nil
}

func (t *sysbenchTesterImpl) outputToResult(output []byte) (*Result, error) {
	return OutputToResult(output, t.serverConfig.GetServerType(), t.serverConfig.GetVersion(), t.test.GetName(), t.test.GetId(), t.suiteId, t.config.GetRuntimeOs(), t.config.GetRuntimeGoArch(), t.serverParams, t.test.GetParamsToSlice(), nil, t.test.GetFromScript())
}

func (t *sysbenchTesterImpl) prepare(ctx context.Context) error {
	cmd := ExecCommand(ctx, sysbenchCommand, t.test.GetPrepareArgs(t.serverConfig)...)
	if t.test.GetFromScript() {
		lp := filepath.Join(t.config.GetScriptDir(), luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf(luaPathEnvVarTemplate, lp))
	}
	return cmd.Run()
}

func (t *sysbenchTesterImpl) run(ctx context.Context) (*Result, error) {
	cmd := exec.CommandContext(ctx, sysbenchCommand, t.test.GetRunArgs(t.serverConfig)...)
	if t.test.GetFromScript() {
		lp := filepath.Join(t.config.GetScriptDir(), luaPath)
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
	cmd := ExecCommand(ctx, sysbenchCommand, t.test.GetCleanupArgs(t.serverConfig)...)
	if t.test.GetFromScript() {
		lp := filepath.Join(t.config.GetScriptDir(), luaPath)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, fmt.Sprintf(luaPathEnvVarTemplate, lp))
	}
	return cmd.Run()
}

func (t *sysbenchTesterImpl) Test(ctx context.Context) (*Result, error) {
	defer t.cleanup(ctx)

	err := t.prepare(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println("Running test", t.test.GetName())

	rs, err := t.run(ctx)
	if err != nil {
		return nil, err
	}

	return rs, nil
}
