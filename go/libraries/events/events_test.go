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
	"testing"

	"github.com/stretchr/testify/assert"

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
)

func TestEvents(t *testing.T) {
	remoteUrl := "https://dolthub.com/org/repo"

	collector := NewCollector()
	testEvent := NewEvent(eventsapi.ClientEventType_CLONE)

	testEvent.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, remoteUrl)

	counter := NewCounter(eventsapi.MetricID_METRIC_UNSPECIFIED)
	counter.Inc()
	testEvent.AddMetric(counter)

	timer := NewTimer(eventsapi.MetricID_METRIC_UNSPECIFIED)
	timer.Stop()
	testEvent.AddMetric(timer)

	collector.CloseEventAndAdd(testEvent)

	assert.Panics(t, func() {
		collector.CloseEventAndAdd(testEvent)
	})

	clientEvents := collector.Close()

	assert.Equal(t, 1, len(clientEvents))
	assert.Equal(t, 1, len(clientEvents[0].Attributes))
	assert.Equal(t, 2, len(clientEvents[0].Metrics))
	assert.NotNil(t, clientEvents[0].StartTime)
	assert.NotNil(t, clientEvents[0].EndTime)

	assert.Equal(t, eventsapi.AttributeID_REMOTE_URL_SCHEME, clientEvents[0].Attributes[0].Id)
	assert.Equal(t, remoteUrl, clientEvents[0].Attributes[0].Value)
	_, isCounter := clientEvents[0].Metrics[0].MetricOneof.(*eventsapi.ClientEventMetric_Count)
	assert.True(t, isCounter)
	_, isTimer := clientEvents[0].Metrics[1].MetricOneof.(*eventsapi.ClientEventMetric_Duration)
	assert.True(t, isTimer)
}
