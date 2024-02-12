package sysbench_runner

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
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type doltServerImpl struct {
	dir                 string
	serverConfig        *ServerConfig
	serverCtx           context.Context
	serverCtxCancelFunc context.CancelFunc
	server              *exec.Cmd
	serverEg            *errgroup.Group
	quit                chan os.Signal
	killSignal          os.Signal
}

var _ Server = &doltServerImpl{}

func NewServer(ctx context.Context, dir string, serverConfig *ServerConfig, killSignal os.Signal, serverParams []string) *doltServerImpl {
	withKeyCtx, cancel := context.WithCancel(ctx)
	gServer, serverCtx := errgroup.WithContext(withKeyCtx)

	server := ExecCommand(serverCtx, serverConfig.ServerExec, serverParams...)
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

func (s *doltServerImpl) Start(ctx context.Context) error {
	if s.serverEg == nil || s.serverCtx == nil || s.quit == nil {
		return ErrServerClosed
	}

	s.serverEg.Go(func() error {
		<-s.quit
		return s.server.Process.Signal(s.killSignal)
	})

	// launch the dolt server
	s.serverEg.Go(func() error {
		return s.server.Run()
	})

	// sleep to allow the server to start
	time.Sleep(10 * time.Second)
	fmt.Println("Successfully started database server")
	return nil
}

func (s *doltServerImpl) Stop(ctx context.Context) error {
	defer s.serverCtxCancelFunc()
	if s.serverEg != nil && s.serverCtx != nil && s.quit != nil {
		// send signal to dolt server
		s.quit <- s.killSignal
		err := s.serverEg.Wait()
		if err != nil {
			// we expect a kill error
			// we only exit in error if this is not the
			// error
			if err.Error() != "signal: killed" {
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
