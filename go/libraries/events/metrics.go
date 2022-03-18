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
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
)

// EventMetric is an interface for getting the eventsapi.ClientEventMetric encoding of a metric
type EventMetric interface {
	// AsClientEventMetrics gets the eventsapi.ClientEventMetric encoding of a metric
	AsClientEventMetric() *eventsapi.ClientEventMetric
}

// Counter is a metric for counting
type Counter struct {
	val      int32
	metricID eventsapi.MetricID
}

// NewCounter creates a new counter
func NewCounter(metricID eventsapi.MetricID) *Counter {
	return &Counter{0, metricID}
}

// Inc incements a counter.  This method happens atomically.
func (c *Counter) Inc() {
	c.Add(1)
}

// Dec decrements a counter. This method happens atomically.
func (c *Counter) Dec() {
	c.Add(-1)
}

// Add a positive or negative value to the current count.
func (c *Counter) Add(addend int32) {
	atomic.AddInt32(&c.val, addend)
}

// AsClientEventMetrics gets the eventsapi.ClientEventMetric encoding of a metric
func (c *Counter) AsClientEventMetric() *eventsapi.ClientEventMetric {
	return &eventsapi.ClientEventMetric{
		MetricId:    c.metricID,
		MetricOneof: &eventsapi.ClientEventMetric_Count{Count: c.val},
	}
}

// Timer a timer is used to time how long something ran for.
type Timer struct {
	start    time.Time
	stop     time.Time
	metricID eventsapi.MetricID
}

// NewTimer creates a new timer and records the start time using the EventNowFunc
func NewTimer(metricID eventsapi.MetricID) *Timer {
	return &Timer{EventNowFunc(), time.Time{}, metricID}
}

// Restart clears the timers end time and sets a new start time using the EventNowFunc
func (t *Timer) Restart() {
	t.start = EventNowFunc()
	t.stop = time.Time{}
}

// Stop sets the end time using the EventNowFunc
func (t *Timer) Stop() *Timer {
	t.stop = EventNowFunc()
	return t
}

// AsClientEventMetrics gets the eventsapi.ClientEventMetric encoding of a metric
func (t *Timer) AsClientEventMetric() *eventsapi.ClientEventMetric {
	if t.stop.Equal(time.Time{}) {
		panic("timer should be stopped before being added as a metric")
	}

	delta := t.stop.Sub(t.start)
	d := durationpb.New(delta)

	return &eventsapi.ClientEventMetric{
		MetricId:    t.metricID,
		MetricOneof: &eventsapi.ClientEventMetric_Duration{Duration: d},
	}
}
