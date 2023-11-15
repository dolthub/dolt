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
	"sync/atomic"
)

// A Service is a runnable unit of functionality that a Controller can
// take responsibility for.  It has an |Init| function, which can error, and
// which should do all of the initialization and validation work necessary to
// bring the service up. It has a |Run| function, which will be called in a
// separate go-routine and should run and provide the functionality associated
// with the service until the |Stop| function is called.
type Service interface {
	Init(context.Context) error
	Run(context.Context)
	Stop() error
}

// AnonService is a simple struct for building Service instances with lambdas
// or funcs, instead of creating an interface implementation.
type AnonService struct {
	InitF func(context.Context) error
	RunF  func(context.Context)
	StopF func() error
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

func (a AnonService) Stop() error {
	if a.StopF == nil {
		return nil
	}
	return a.StopF()
}

// ServiceState is a small abstraction so that a service implementation can
// easily track what state it is in and can make decisions about what to do
// based on what state it is coming from. In particular, it's not rare for a
// |Close| implementation to need to do something different based on whether
// the service is only init'd or if it is running. It's also not rare for a
// service to decide it needs to do nothing in Init, in which case it can leave
// the service in Off, and the Run and Close methods can check that to ensure
// they do not do anything either.
type ServiceState uint32

const (
	ServiceState_Off ServiceState = iota
	ServiceState_Init
	ServiceState_Run
	ServiceState_Stopped
)

func (ss *ServiceState) Swap(new ServiceState) (old ServiceState) {
	return ServiceState(atomic.SwapUint32((*uint32)(ss), uint32(new)))
}

func (ss *ServiceState) CompareAndSwap(old, new ServiceState) (swapped bool) {
	return atomic.CompareAndSwapUint32((*uint32)(ss), uint32(old), uint32(new))
}

// A Controller is responsible for initializing a number of registered
// services, running them all, and stopping them all when requested. Services
// are registered with |Register(*Service)|. When |Start| is called, the
// services are all initialized,  in the order of their registration, and if
// every service initializes successfully, they are |Run| concurrently. When
// |Stop| is called, services are stopped in reverse-registration order. |Stop|
// does not block for the goroutines spawned by |Start| to complete, although
// typically a Service's |Stop| function should do that. |Stop| only returns an
// error if the Controller is in an illegal state where it is not valid to Stop
// it. In particular, it does not return an error seen by a Service on Stop().
// That error is returned from Start() and from WaitForStop().
//
// Any attempt to register a service after |Start| or |Stop| has been called
// will return an error.
//
// If an error occurs when initializing the services of a Controller, the
// Stop functions of any already initialized Services are called in
// reverse-order. The error which caused the initialization error is returned.
//
// In the case that all Services Init successfully, the error returned from
// |Start| is the first non-nil error which is returned from the |Stop|
// functions, in the order they are called.
//
// WaitForStart() can be called at any time on a Controller. It will
// block until |Start| is called. After |Start| is called, if all the services
// succesfully initialize, it will return |nil|. Otherwise it will return the
// same error |Start| returned.
//
// WaitForStop() can be called at any time on a Controller. It will block
// until |Start| is called and initialization fails, or until |Stop| is called.
// It will return the same error which |Start| returned.
type Controller struct {
	mu        sync.Mutex
	services  []Service
	initErr   error
	stopErr   error
	started   bool
	startCh   chan struct{}
	stopped   bool
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

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
	if c.started {
		c.mu.Unlock()
		return errors.New("Controller: cannot Register a service on a controller which was already started")
	}
	c.services = append(c.services, svc)
	c.mu.Unlock()
	return nil
}

func (c *Controller) Stop() error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return errors.New("Controller: cannot Stop a controller which was never started")
	}
	if c.stopped {
		c.mu.Unlock()
		return errors.New("Controller: cannot Stop a controller which was already stopped or which failed to initialize all its services")
	}
	c.stopped = true
	close(c.stopCh)
	c.mu.Unlock()
	<-c.stoppedCh
	return nil
}

func (c *Controller) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		return errors.New("Controller: cannot start service controller twice")
	}
	c.started = true
	svcs := make([]Service, len(c.services))
	copy(svcs, c.services)
	c.mu.Unlock()
	for i, s := range svcs {
		err := s.Init(ctx)
		if err != nil {
			for j := i - 1; j >= 0; j-- {
				svcs[j].Stop()
			}
			c.mu.Lock()
			c.stopped = true
			c.initErr = err
			close(c.startCh)
			close(c.stoppedCh)
			c.mu.Unlock()
			return err
		}
	}
	close(c.startCh)
	for _, s := range svcs {
		go s.Run(ctx)
	}
	<-c.stopCh
	var stopErr error
	for i := len(svcs) - 1; i >= 0; i-- {
		err := svcs[i].Stop()
		if err != nil && stopErr == nil {
			stopErr = err
		}
	}
	c.mu.Lock()
	if stopErr != nil {
		c.stopErr = stopErr
	}
	close(c.stoppedCh)
	c.mu.Unlock()
	return stopErr
}
