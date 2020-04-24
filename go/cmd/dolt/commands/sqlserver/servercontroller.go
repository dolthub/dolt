// Copyright 2019 Liquidata, Inc.
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

package sqlserver

import (
	"sync"
)

type ServerController struct {
	//serverClosed    *sync.WaitGroup
	//serverStarted   *sync.WaitGroup
	startCh         chan struct{}
	closeCh         chan struct{}
	closeCalled     *sync.Once
	closeRegistered *sync.Once
	stopRegistered  *sync.Once
	closeFunction   func() error
	startError      error
	closeError      error
}

// CreateServerController creates a `ServerController` for use with synchronizing on `Serve`.
func CreateServerController() *ServerController {
	sc := &ServerController{
		startCh:         make(chan struct{}),
		closeCh:         make(chan struct{}),
		closeCalled:     &sync.Once{},
		closeRegistered: &sync.Once{},
		stopRegistered:  &sync.Once{},
	}
	return sc
}

// registerCloseFunction is called within `Serve` to associate the close function with a future `StopServer` call.
// Only the first call will register and unblock, thus it is safe to be called multiple times.
func (controller *ServerController) registerCloseFunction(startError error, closeFunc func() error) {
	controller.closeRegistered.Do(func() {
		if startError != nil {
			controller.startError = startError
		}
		controller.closeFunction = closeFunc
		close(controller.startCh)
	})
}

// serverStopped is called within `Serve` to signal that the server has stopped and set the exit code.
// Only the first call will register and unblock, thus it is safe to be called multiple times.
func (controller *ServerController) serverStopped(closeError error) {
	controller.stopRegistered.Do(func() {
		if closeError != nil {
			controller.closeError = closeError
		}
		close(controller.closeCh)
	})
}

// StopServer stops the server if it is running. Only the first call will trigger the stop, thus it is safe for
// multiple goroutines to call this function.
func (controller *ServerController) StopServer() {
	if controller.closeFunction != nil {
		controller.closeCalled.Do(func() {
			if err := controller.closeFunction(); err != nil {
				controller.closeError = err
			}
		})
	}
}

// WaitForClose blocks the caller until the server has closed. The return is the last error encountered, if any.
func (controller *ServerController) WaitForClose() error {
	select {
	case <-controller.closeCh:
		break
	}

	return controller.closeError
}

// WaitForStart blocks the caller until the server has started. An error is returned if one was encountered.
func (controller *ServerController) WaitForStart() error {
	select {
	case <-controller.startCh:
		break
	case <-controller.closeCh:
		break
	}

	return controller.startError
}
