// Copyright 2019 Dolthub, Inc.
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

package events

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/denisbrodbeck/machineid"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

var machineID = "invalid"
var machineIDOnce = &sync.Once{}

// getMachineID returns a unique machine identifier hash specific to dolt
func getMachineID() string {
	machineIDOnce.Do(func() {
		id, err := machineid.ProtectedID("dolt")

		if err == nil {
			machineID = id
		}
	})

	return machineID
}

// GlobalCollector is an instance of a collector where all events should be sent via the CloseEventAndAdd function
var globalCollector = NewCollector("invalid", nil)
var globalMu *sync.Mutex = &sync.Mutex{}

func GlobalCollector() *Collector {
	globalMu.Lock()
	defer globalMu.Unlock()
	return globalCollector
}

func SetGlobalCollector(c *Collector) {
	globalMu.Lock()
	defer globalMu.Unlock()
	cur := globalCollector
	globalCollector = c
	toTransfer := cur.Close()
	for _, e := range toTransfer {
		globalCollector.evtCh <- e
	}
}

const collChanBufferSize = 32
const maxBatchedEvents = 64

// Collector collects and stores Events later to be sent to an Emitter.
type Collector struct {
	events []*eventsapi.ClientEvent
	wg     sync.WaitGroup
	evtCh  chan *eventsapi.ClientEvent
	st     *sendingThread
}

// NewCollector creates a new instance of a collector
func NewCollector(version string, emitter Emitter) *Collector {
	evtCh := make(chan *eventsapi.ClientEvent, collChanBufferSize)

	c := &Collector{
		evtCh: evtCh,
		st:    newSendingThread(version, emitter),
	}

	c.st.start()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for evt := range c.evtCh {
			c.events = append(c.events, evt)
			if len(c.events) >= maxBatchedEvents {
				c.st.batchCh <- c.events
				c.events = nil
			}
		}
		if len(c.events) > 0 {
			c.st.batchCh <- c.events
			c.events = nil
		}
		c.events = c.st.stop()
	}()

	return c
}

// CloseEventAndAdd closes the supplied event and adds it to the collection of events.  This method is thread safe.
func (c *Collector) CloseEventAndAdd(evt *Event) {
	c.evtCh <- evt.close()
}

// Close waits for any remaining events to finish collection and then returns a slice of ClientEvents to be passed to an
// emitter.
func (c *Collector) Close() []*eventsapi.ClientEvent {
	close(c.evtCh)

	c.wg.Wait()

	return c.events
}

type sendingThread struct {
	logCtx  context.Context
	cancelF func()

	batchCh chan []*eventsapi.ClientEvent
	unsent  []*eventsapi.ClientEvent
	version string

	emitter Emitter

	wg sync.WaitGroup
}

func newSendingThread(version string, emitter Emitter) *sendingThread {
	ctx, cancel := context.WithCancel(context.Background())
	return &sendingThread{
		logCtx:  ctx,
		cancelF: cancel,
		batchCh: make(chan []*eventsapi.ClientEvent, 8),
		version: version,
		emitter: emitter,
	}
}

func (s *sendingThread) start() {
	s.wg.Add(1)
	go s.run()
}

func (s *sendingThread) stop() []*eventsapi.ClientEvent {
	s.cancelF()
	close(s.batchCh)
	s.wg.Wait()
	return s.unsent
}

func (s *sendingThread) run() {
	defer s.wg.Done()

	var timer *time.Timer

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = time.Second
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = 0

	for {
		var timerCh <-chan time.Time
		if timer != nil {
			timerCh = timer.C
		}
		select {
		case batch, ok := <-s.batchCh:
			if !ok {
				if s.emitter != nil && len(s.unsent) > 0 {
					err := s.emitter.LogEvents(s.logCtx, s.version, s.unsent)
					if err == nil {
						s.unsent = nil
					}
				}
				return
			}
			s.unsent = append(s.unsent, batch...)
			if s.emitter != nil {
				if timer != nil && !timer.Stop() {
					<-timer.C
					timer.Reset(0)
				} else {
					timer = time.NewTimer(0)
				}
			}
		case <-timerCh:
			err := s.emitter.LogEvents(s.logCtx, s.version, s.unsent)
			if err == nil {
				s.unsent = nil
				bo.Reset()
				timer = nil
			} else {
				timer.Reset(bo.NextBackOff())
			}
		}
	}
}
