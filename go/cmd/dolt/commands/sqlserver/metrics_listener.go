// Copyright 2021 Dolthub, Inc.
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

package sqlserver

import (
	"time"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/prometheus/client_golang/prometheus"
)

var _ server.ServerEventListener = (*metricsListener)(nil)

type metricsListener struct {
	cntConnections         prometheus.Counter
	cntDisconnects         prometheus.Counter
	gaugeConcurrentConn    prometheus.Gauge
	gaugeConcurrentQueries prometheus.Gauge
	histQueryDur           prometheus.Histogram
}

func newMetricsListener(labels prometheus.Labels) *metricsListener {
	return &metricsListener{
		cntConnections: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "dss_connects",
			Help:        "Count of server connects",
			ConstLabels: labels,
		}),
		cntDisconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "dss_disconnects",
			Help:        "Count of server disconnects",
			ConstLabels: labels,
		}),
		gaugeConcurrentConn: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "dss_concurrent_connections",
			Help:        "Number of clients concurrently connected to this instance of dolt sql server",
			ConstLabels: labels,
		}),
		gaugeConcurrentQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "dss_concurrent_queries",
			Help:        "Number of queries concurrently being run on this instance of dolt sql server",
			ConstLabels: labels,
		}),
		histQueryDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "dss_query_duration",
			Help:        "Histogram of dolt sql server query runtimes",
			ConstLabels: labels,
			Buckets:     []float64{0.01, 0.1, 1.0, 10.0, 100.0, 1000.0}, // 10 ms to 16 mins 40 secs
		}),
	}
}

func (m *metricsListener) ClientConnected() {
	m.gaugeConcurrentConn.Add(1.0)
	m.cntConnections.Add(1.0)
}

func (m *metricsListener) ClientDisconnected() {
	m.gaugeConcurrentConn.Sub(1.0)
	m.cntDisconnects.Add(1.0)
}

func (m *metricsListener) QueryStarted() {
	m.gaugeConcurrentQueries.Add(1.0)
}

func (m *metricsListener) QueryCompleted(success bool, duration time.Duration) {
	m.gaugeConcurrentQueries.Sub(1.0)
	m.histQueryDur.Observe(duration.Seconds())
}
