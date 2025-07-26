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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

func TestCounterAtomicity(t *testing.T) {
	c := NewCounter(eventsapi.MetricID_METRIC_UNSPECIFIED)
	wg := &sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			c.Add(10)
			time.Sleep(time.Millisecond)
			c.Inc()
			time.Sleep(time.Millisecond)
			c.Add(-5)
			time.Sleep(time.Millisecond)
			c.Dec()
		}()
	}

	wg.Wait()
	cem := c.AsClientEventMetric()

	assert.Equal(t, int32(50), cem.GetCount())
}

func TestTimer(t *testing.T) {
	EventNowFunc = func() time.Time { return time.Date(2018, 8, 6, 10, 0, 0, 0, time.UTC) }
	timer := NewTimer(eventsapi.MetricID_METRIC_UNSPECIFIED)
	EventNowFunc = func() time.Time { return time.Date(2018, 8, 6, 10, 1, 0, 0, time.UTC) }
	timer.Restart()
	EventNowFunc = func() time.Time { return time.Date(2018, 8, 6, 10, 1, 5, 123, time.UTC) }
	timer.Stop()
	EventNowFunc = time.Now

	cem := timer.AsClientEventMetric()
	assert.Equal(t, int64(5), cem.GetDuration().Seconds)
	assert.Equal(t, int32(123), cem.GetDuration().Nanos)
}

func TestPanicOnAddUnstoppedTimer(t *testing.T) {
	assert.Panics(t, func() {
		timer := NewTimer(eventsapi.MetricID_METRIC_UNSPECIFIED)
		timer.AsClientEventMetric()
	})
}
