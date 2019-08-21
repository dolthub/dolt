package events

import (
	"sync"
	"sync/atomic"
	"time"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

var GlobalCollector = NewCollector()

const collChanBufferSize = 32

type Collector struct {
	val   *atomic.Value
	wg    *sync.WaitGroup
	evtCh chan *eventsapi.ClientEvent
}

func NewCollector() *Collector {
	wg := &sync.WaitGroup{}
	evtCh := make(chan *eventsapi.ClientEvent, collChanBufferSize)

	c := &Collector{&atomic.Value{}, wg, evtCh}

	wg.Add(1)
	go func() {
		defer wg.Done()

		var events []*eventsapi.ClientEvent
		for evt := range evtCh {
			events = append(events, evt)
		}

		c.val.Store(events)
	}()

	return c
}

func (c *Collector) CloseEventAndAdd(evt *Event) {
	ce := evt.close()
	c.evtCh <- ce
}

func (c *Collector) Close() []*eventsapi.ClientEvent {
	close(c.evtCh)

	c.wg.Wait()

	var interf interface{}
	for interf = c.val.Load(); interf == nil; interf = c.val.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	return interf.([]*eventsapi.ClientEvent)
}
