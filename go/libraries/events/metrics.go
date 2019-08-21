package events

import (
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/duration"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

type EventMetric interface {
	AsClientEventMetric() *eventsapi.ClientEventMetric
}

type Counter struct {
	val      int32
	metricID eventsapi.MetricID
}

func NewCounter(metricID eventsapi.MetricID) *Counter {
	return &Counter{0, metricID}
}

func (c *Counter) Inc() {
	c.Add(1)
}

func (c *Counter) Dec() {
	c.Add(-1)
}

func (c *Counter) Add(addend int32) {
	atomic.AddInt32(&c.val, addend)
}

func (c *Counter) AsClientEventMetric() *eventsapi.ClientEventMetric {
	return &eventsapi.ClientEventMetric{
		MetricId:    c.metricID,
		MetricOneof: &eventsapi.ClientEventMetric_Count{Count: c.val},
	}
}

type Timer struct {
	start    time.Time
	stop     time.Time
	metricID eventsapi.MetricID
}

func NewTimer(metricID eventsapi.MetricID) *Timer {
	return &Timer{EventNowFunc(), time.Time{}, metricID}
}

func (t *Timer) ReStart() {
	t.start = EventNowFunc()
	t.stop = time.Time{}
}

func (t *Timer) Stop() *Timer {
	t.stop = EventNowFunc()
	return t
}

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
