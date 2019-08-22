package events

import (
	"github.com/denisbrodbeck/machineid"
	"log"
	"sync"
	"sync/atomic"
	"time"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

// getMachineID returns a unique machine identifier hash specific to dolt
func getMachineID() string {
	id, err := machineid.ProtectedID("dolt")
	if err != nil {
		log.Fatal(err)
	}

	return id
}

// GlobalCollector is an instance of a collector where all events should be sent via the CloseEventAndAdd function
var GlobalCollector = NewCollector()

const collChanBufferSize = 32

// Collector collects and stores Events later to be sent to an Emitter.
type Collector struct {
	val   *atomic.Value
	wg    *sync.WaitGroup
	evtCh chan *eventsapi.ClientEvent
}

// NewCollector creates a new instance of a collector
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

// CloseEventAndAdd closes the supplied event and adds it to the collection of events.  This method is thread safe.
func (c *Collector) CloseEventAndAdd(evt *Event) {
	c.evtCh <- evt.close()
}

// Close waits for any remaining events to finish collection and then returns a slice of ClientEvents to be passed to an
// emitter.
func (c *Collector) Close() []*eventsapi.ClientEvent {
	close(c.evtCh)

	c.wg.Wait()

	var interf interface{}
	for interf = c.val.Load(); interf == nil; interf = c.val.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	return interf.([]*eventsapi.ClientEvent)
}
