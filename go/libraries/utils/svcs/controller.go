// Copyright 2023 Dolthub, Inc.
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

package svcs

import (
	"context"
	"errors"
	"sync"
)

// RunState indicates whether the controller invoked a service's Run method
// before calling Stop. Stop implementations should use this to skip cleanup
// that depends on Run having executed (e.g. waiting on a goroutine that was
// only started inside Run).
type RunState int

const (
	// RunNotInvoked means the controller did not call Run on this service.
	// This happens when initialization fails before all services are started,
	// or when the controller is stopped before transitioning to the running state.
	RunNotInvoked RunState = iota
	// RunInvoked means the controller called Run on this service.
	RunInvoked
)

// A Service is a runnable unit of functionality that a Controller can
// take responsibility for.  It has an |Init| function, which can error, and
// which should do all of the initialization and validation work necessary to
// bring the service up. It has a |Run| function, which will be called in a
// separate go-routine and should run and provide the functionality associated
// with the service until the |Stop| function is called.
//
// Stop receives a RunState indicating whether Run was invoked. When
// RunNotInvoked, Stop should only undo work done in Init (e.g. close a
// listener that was created). When RunInvoked, Stop should also wait for the
// Run goroutine to complete if necessary.
type Service interface {
	Init(context.Context) error
	Run(context.Context)
	Stop(RunState) error
}

// AnonService is a simple struct for building Service instances with lambdas
// or funcs, instead of creating an interface implementation.
type AnonService struct {
	InitF func(context.Context) error
	RunF  func(context.Context)
	StopF func(RunState) error
}

func (a AnonService) Init(ctx context.Context) error {
	if a.InitF == nil {
		return nil
	}
	return a.InitF(ctx)
}

func (a AnonService) Run(ctx context.Context) {
	if a.RunF == nil {
		return
	}
	a.RunF(ctx)
}

func (a AnonService) Stop(rs RunState) error {
	if a.StopF == nil {
		return nil
	}
	return a.StopF(rs)
}

// A Controller is responsible for initializing a number of registered
// services, running them all, and stopping them all when requested. Services
// are registered with |Register(Service)|. When |Start| is called, the
// services are all initialized, in the order of their registration, and if
// every service initializes successfully, they are |Run| concurrently. When
// |Stop| is called, services are stopped in reverse-registration order. |Stop|
// returns once the corresponding |Stop| method on all successfully |Init|ed
// services has returned. |Stop| does not explicitly block for the goroutines
// where the |Run| methods are called to complete.  A Service's |Stop| function
// should use the |RunState| parameter to determine whether |Run| was invoked
// and wait for it to complete if necessary.
//
// Any attempt to register a service after |Start| or |Stop| has been called
// will return an error.
//
// If an error occurs when initializing the services of a Controller, the
// Stop functions of any already initialized Services are called in
// reverse-order with RunNotInvoked. The error which caused the initialization
// error is returned.
//
// In the case that all Services Init successfully, the error returned from
// |Start| is the first non-nil error which is returned from the |Stop|
// functions, in the order they are called.
//
// If |Stop| is called before |Start|, |Start| will return an error. |Register|
// will also begin returning an error after |Stop| is called, if it is called
// before |Start|.
//
// |WaitForStart| can be called at any time on a Controller. It will block
// until |Start| is called. After |Start| is called, if all the services
// successfully initialize, it will return |nil|. Otherwise it will return the
// same error |Start| returned.
//
// |WaitForStop| can be called at any time on a Controller. It will block until
// |Start| is called and initialization fails, or until |Stop| is called.  It
// will return the same error which |Start| returned.
type Controller struct {
	mu        sync.Mutex
	services  []Service
	initErr   error
	stopErr   error
	startCh   chan struct{}
	stopCh    chan struct{}
	stoppedCh chan struct{}
	state     controllerState
}

type controllerState int

const (
	controllerState_created  controllerState = iota
	controllerState_starting controllerState = iota
	controllerState_running  controllerState = iota
	controllerState_stopping controllerState = iota
	controllerState_stopped  controllerState = iota
)

func NewController() *Controller {
	return &Controller{
		startCh:   make(chan struct{}),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

func (c *Controller) WaitForStart() error {
	<-c.startCh
	c.mu.Lock()
	err := c.initErr
	c.mu.Unlock()
	return err
}

func (c *Controller) WaitForStop() error {
	<-c.stoppedCh
	c.mu.Lock()
	var err error
	if c.initErr != nil {
		err = c.initErr
	} else if c.stopErr != nil {
		err = c.stopErr
	}
	c.mu.Unlock()
	return err
}

func (c *Controller) Register(svc Service) error {
	c.mu.Lock()
	if c.state != controllerState_created {
		c.mu.Unlock()
		return errors.New("Controller: cannot Register a service on a controller which was already started or stopped")
	}
	c.services = append(c.services, svc)
	c.mu.Unlock()
	return nil
}

func (c *Controller) Stop() {
	c.mu.Lock()
	if c.state == controllerState_created {
		// Nothing ever ran, we can transition directly to stopped.
		// TODO: Is a more correct contract to put an error into initErr here? The services never started successfully...
		c.state = controllerState_stopped
		close(c.startCh)
		close(c.stoppedCh)
		c.mu.Unlock()
		return
	} else if c.state == controllerState_stopped {
		// We already stopped.
		c.mu.Unlock()
		return
	} else if c.state != controllerState_stopping {
		// We should only do this transition once. We signal to |Start|
		// by closing the |stopCh|.
		close(c.stopCh)
		c.state = controllerState_stopping
		c.mu.Unlock()
	}
	<-c.stoppedCh
}

func (c *Controller) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.state != controllerState_created {
		c.mu.Unlock()
		return errors.New("Controller: cannot start service controller after is has been started or stopped")
	}
	c.state = controllerState_starting
	svcs := make([]Service, len(c.services))
	copy(svcs, c.services)
	c.mu.Unlock()
	for i, s := range svcs {
		err := s.Init(ctx)
		if err != nil {
			for j := i - 1; j >= 0; j-- {
				svcs[j].Stop(RunNotInvoked)
			}
			c.mu.Lock()
			c.state = controllerState_stopped
			c.initErr = err
			close(c.startCh)
			close(c.stoppedCh)
			c.mu.Unlock()
			return err
		}
	}
	close(c.startCh)
	c.mu.Lock()
	runInvoked := false
	if c.state == controllerState_starting {
		c.state = controllerState_running
		c.mu.Unlock()
		for _, s := range svcs {
			go s.Run(ctx)
		}
		runInvoked = true
		<-c.stopCh
	} else {
		// We were stopped while initializing. Start shutting things down.
		c.mu.Unlock()
	}
	rs := RunNotInvoked
	if runInvoked {
		rs = RunInvoked
	}
	var stopErr error
	for i := len(svcs) - 1; i >= 0; i-- {
		err := svcs[i].Stop(rs)
		if err != nil && stopErr == nil {
			stopErr = err
		}
	}
	c.mu.Lock()
	if stopErr != nil {
		c.stopErr = stopErr
	}
	c.state = controllerState_stopped
	close(c.stoppedCh)
	c.mu.Unlock()
	return stopErr
}
