package sqlserver

import (
	"sync"
)

type ServerController struct {
	serverClosed    *sync.WaitGroup
	serverStarted   *sync.WaitGroup
	closeCalled     *sync.Once
	closeRegistered *sync.Once
	stopRegistered  *sync.Once
	closeFunction   func() error
	startError      error
	closeError      error
}

// CreateServerController creates a `ServerController` for use with synchronizing on `serve`.
func CreateServerController() *ServerController {
	sc := &ServerController{
		serverClosed:    &sync.WaitGroup{},
		serverStarted:   &sync.WaitGroup{},
		closeCalled:     &sync.Once{},
		closeRegistered: &sync.Once{},
		stopRegistered:  &sync.Once{},
	}
	sc.serverClosed.Add(1)
	sc.serverStarted.Add(1)
	return sc
}

// registerCloseFunction is called within `serve` to associate the close function with a future `StopServer` call.
// Only the first call will register and unblock, thus it is safe to be called multiple times.
func (controller *ServerController) registerCloseFunction(startError error, closeFunc func() error) {
	controller.closeRegistered.Do(func() {
		if startError != nil {
			controller.startError = startError
		}
		controller.closeFunction = closeFunc
		controller.serverStarted.Done()
	})
}

// serverStopped is called within `serve` to signal that the server has stopped and set the exit code.
// Only the first call will register and unblock, thus it is safe to be called multiple times.
func (controller *ServerController) serverStopped(closeError error) {
	controller.stopRegistered.Do(func(){
		if closeError != nil {
			controller.closeError = closeError
		}
		controller.serverClosed.Done()
	})
}

// StopServer stops the server if it is running. Only the first call will trigger the stop, thus it is safe for
// multiple goroutines to call this function.
func (controller *ServerController) StopServer() {
	if controller.closeFunction != nil {
		controller.closeCalled.Do(func(){
			if err := controller.closeFunction(); err != nil {
				controller.closeError = err
			}
		})
	}
}

// WaitForClose blocks the caller until the server has closed. The return is the last error encountered, if any.
func (controller *ServerController) WaitForClose() error {
	controller.serverClosed.Wait()
	return controller.closeError
}

// WaitForStart blocks the caller until the server has started. An error is returned if one was encountered.
func (controller *ServerController) WaitForStart() error {
	controller.serverStarted.Wait()
	return controller.startError
}