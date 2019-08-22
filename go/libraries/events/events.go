package events

import (
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

// EventNowFunc function is used to get the current time and can be overridden for testing.
var EventNowFunc = time.Now

// Event is an event to be added to a collector and logged
type Event struct {
	ce         *eventsapi.ClientEvent
	m          *sync.Mutex
	attributes map[eventsapi.AttributeID]string
}

// NewEvent creates an Event of a given type.  The event creation time is recorded as the start time for the event.
// When the event is passed to a collector's CloseEventAndAdd method the end time of the event is recorded
func NewEvent(ceType eventsapi.ClientEventType) *Event {
	return &Event{
		ce: &eventsapi.ClientEvent{
			Id:        uuid.New().String(),
			StartTime: &timestamp.Timestamp{Seconds: int64(EventNowFunc().Second())},
			Type:      ceType,
		},
		m:          &sync.Mutex{},
		attributes: make(map[eventsapi.AttributeID]string),
	}
}

// AddMetric adds a metric to the event.  This method is thread safe.
func (evt *Event) AddMetric(em EventMetric) {
	evt.m.Lock()
	defer evt.m.Unlock()

	evt.ce.Metrics = append(evt.ce.Metrics, em.AsClientEventMetric())
}

// AddAttribute adds an attribute to the event.  This method is thread safe
func (evt *Event) AddAttribute(attID eventsapi.AttributeID, attVal string) {
	evt.m.Lock()
	defer evt.m.Unlock()

	evt.attributes[attID] = attVal
}

func (evt *Event) close() *eventsapi.ClientEvent {
	if evt.ce == nil {
		panic("multiple close calls for the same event.")
	}

	evt.m.Lock()
	defer evt.m.Unlock()

	for k, v := range evt.attributes {
		evt.ce.Attributes = append(evt.ce.Attributes, &eventsapi.ClientEventAttribute{Id: k, Value: v})
	}

	ce := evt.ce
	evt.ce = nil

	return ce
}
