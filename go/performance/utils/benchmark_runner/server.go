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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	"golang.org/x/sync/errgroup"
)

var ErrServerClosed = errors.New("server was previously closed")

type Server interface {
	Start() error
	Stop() error
	WithEnv(key, value string)
}

type doltServerImpl struct {
	dir                 string
	serverConfig        ServerConfig
	serverCtx           context.Context
	serverCtxCancelFunc context.CancelFunc
	server              *exec.Cmd
	serverEg            *errgroup.Group
	quit                chan os.Signal
	killSignal          os.Signal
}

var _ Server = &doltServerImpl{}

func NewServer(ctx context.Context, dir string, serverConfig ServerConfig, killSignal os.Signal, serverParams []string) *doltServerImpl {
	withKeyCtx, cancel := context.WithCancel(ctx)
	gServer, serverCtx := errgroup.WithContext(withKeyCtx)

	server := ExecCommand(serverCtx, serverConfig.GetServerExec(), serverParams...)
	server.Dir = dir

	quit := make(chan os.Signal, 1)
	return &doltServerImpl{
		dir:                 dir,
		serverConfig:        serverConfig,
		serverCtx:           serverCtx,
		server:              server,
		serverCtxCancelFunc: cancel,
		serverEg:            gServer,
		quit:                quit,
		killSignal:          killSignal,
	}
}

func (s *doltServerImpl) WithEnv(key, val string) {
	if s.server != nil {
		s.server.Env = append(s.server.Env, fmt.Sprintf("%s=%s", key, val))
	}
}

func (s *doltServerImpl) Start() error {
	if s.serverEg == nil || s.serverCtx == nil || s.quit == nil {
		return ErrServerClosed
	}

	s.serverEg.Go(func() error {
		<-s.quit
		return s.server.Process.Signal(s.killSignal)
	})

	s.serverEg.Go(func() error {
		if Debug {
			fmt.Println("DUSTIN: starting server in debug mode with logs to stdout/err")
			s.server.Stdout = os.Stdout
			s.server.Stderr = os.Stderr
		}
		return s.server.Run()
	})

	// sleep to allow the server to start
	time.Sleep(10 * time.Second)
	fmt.Println("Successfully started database server")
	return nil
}

func (s *doltServerImpl) Stop() error {
	defer s.serverCtxCancelFunc()
	if s.serverEg != nil && s.serverCtx != nil && s.quit != nil {
		// send signal to dolt server
		s.quit <- s.killSignal
		err := s.serverEg.Wait()
		if err != nil {
			// we expect a kill error
			// we only exit in error if this is not the
			// error
			if err.Error() != expectedServerKilledErrorMessage && err.Error() != expectedServerTerminatedErrorMessage {
				fmt.Println(err)
				close(s.quit)
				return err
			}
		}

		fmt.Println("Successfully killed database server")
		close(s.quit)

		s.quit = nil
		s.serverCtx = nil
		s.serverEg = nil
		s.serverCtxCancelFunc = func() {}
	}

	return nil
}
