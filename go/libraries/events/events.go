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

package events

import (
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
)

// EventNowFunc function is used to get the current time and can be overridden for testing.
var EventNowFunc = time.Now

func nowTimestamp() *timestamp.Timestamp {
	now := EventNowFunc()
	ts, err := ptypes.TimestampProto(now)

	if err != nil {
		panic(err)
	}

	return ts
}

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
			StartTime: nowTimestamp(),
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

// SetAttribute adds an attribute to the event.  This method is thread safe
func (evt *Event) SetAttribute(attID eventsapi.AttributeID, attVal string) {
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

	evt.ce.EndTime = nowTimestamp()

	for k, v := range evt.attributes {
		evt.ce.Attributes = append(evt.ce.Attributes, &eventsapi.ClientEventAttribute{Id: k, Value: v})
	}

	ce := evt.ce
	evt.ce = nil

	return ce
}

// GetAttribute gets an attribute from the Event. This method is thread safe
func (evt *Event) GetAttribute(attID eventsapi.AttributeID) string {
	evt.m.Lock()
	defer evt.m.Unlock()

	if val, ok := evt.attributes[attID]; ok {
		return val
	}

	return ""
}

// GetClientEventType returns the Event's ClientEvent type. This method is thread safe
func (evt *Event) GetClientEventType() eventsapi.ClientEventType {
	evt.m.Lock()
	defer evt.m.Unlock()

	return evt.ce.Type
}
