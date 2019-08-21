package events

import (
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/duration"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
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
	seconds := int64(delta.Seconds())
	nanos := int32(delta.Nanoseconds() % 1000000000)
	d := duration.Duration{Seconds: seconds, Nanos: nanos}

	return &eventsapi.ClientEventMetric{
		MetricId:    t.metricID,
		MetricOneof: &eventsapi.ClientEventMetric_Duration{Duration: &d},
	}
}
