package events

import (
	"testing"

	"github.com/stretchr/testify/assert"

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi_v1alpha1"
)

func TestEvents(t *testing.T) {
	remoteUrl := "https://dolthub.com/org/repo"

	collector := NewCollector()
	testEvent := NewEvent(eventsapi.ClientEventType_CLONE)

	testEvent.SetAttribute(eventsapi.AttributeID_REMOTEURL, remoteUrl)

	counter := NewCounter(eventsapi.MetricID_UNSPECIFIED_METRIC)
	counter.Inc()
	testEvent.AddMetric(counter)

	timer := NewTimer(eventsapi.MetricID_UNSPECIFIED_METRIC)
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

	assert.Equal(t, eventsapi.AttributeID_REMOTEURL, clientEvents[0].Attributes[0].Id)
	assert.Equal(t, remoteUrl, clientEvents[0].Attributes[0].Value)
	_, isCounter := clientEvents[0].Metrics[0].MetricOneof.(*eventsapi.ClientEventMetric_Count)
	assert.True(t, isCounter)
	_, isTimer := clientEvents[0].Metrics[1].MetricOneof.(*eventsapi.ClientEventMetric_Duration)
	assert.True(t, isTimer)
}
