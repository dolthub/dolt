package sysbench_runner

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"time"
)

type doltServerImpl struct {
	dir                 string
	serverConfig        *ServerConfig
	serverCtx           context.Context
	serverCtxCancelFunc context.CancelFunc
	server              *exec.Cmd
	serverEg            *errgroup.Group
	quit                chan os.Signal
	wg                  *sync.WaitGroup
	killSignal          os.Signal
}

var _ Server = &doltServerImpl{}

func NewDoltServer(ctx context.Context, dir string, serverConfig *ServerConfig, killSignal os.Signal, serverParams []string) *doltServerImpl {
	withKeyCtx, cancel := context.WithCancel(ctx)
	gServer, serverCtx := errgroup.WithContext(withKeyCtx)

	server := ExecCommand(serverCtx, serverConfig.ServerExec, serverParams...)
	server.Dir = dir

	quit := make(chan os.Signal, 1)
	return &doltServerImpl{
		dir:                 dir,
		serverConfig:        serverConfig,
		server:              server,
		serverCtxCancelFunc: cancel,
		serverEg:            gServer,
		quit:                quit,
		wg:                  &sync.WaitGroup{},
		killSignal:          killSignal,
	}
}

func (s *doltServerImpl) Start(ctx context.Context) error {
	signal.Notify(s.quit, os.Interrupt, s.killSignal)
	s.wg.Add(1)
	go func() {
		<-s.quit
		defer s.wg.Done()
		signal.Stop(s.quit)
		// todo: remove this ?? s.serverCtxCancelFunc()
	}()

	// launch the dolt server
	s.serverEg.Go(func() error {
		return s.server.Run()
	})
	// sleep to allow the server to start
	time.Sleep(5 * time.Second)
	return nil
}

func (s *doltServerImpl) Stop(ctx context.Context) error {
	defer s.serverCtxCancelFunc()
	if s.wg != nil && s.serverEg != nil && s.serverCtx != nil && s.quit != nil {
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
				s.wg.Wait()
				return err
			}
		}

		fmt.Println("Successfully killed server")
		close(s.quit)
		s.wg.Wait()

		s.wg = nil
		s.quit = nil
		s.serverCtx = nil
		s.serverEg = nil
		s.serverCtxCancelFunc = func() {}
	}

	return nil
}
