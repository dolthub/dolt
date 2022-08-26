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
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/libraries/utils/version"

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
	gaugeVersion           prometheus.Gauge
}

func newMetricsListener(labels prometheus.Labels, versionStr string) (*metricsListener, error) {
	ml := &metricsListener{
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
		gaugeVersion: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "dss_dolt_version",
			Help:        "The version of dolt currently running on the machine",
			ConstLabels: labels,
		}),
	}

	u32Version, err := version.Encode(versionStr)
	if err != nil {
		return nil, err
	}

	f64Version := float64(u32Version)
	decoded := version.Decode(uint32(f64Version))
	if decoded != versionStr {
		return nil, fmt.Errorf("the float64 encoded version does not decode back to its original value. version:'%s', decoded:'%s'", versionStr, decoded)
	}

	prometheus.MustRegister(ml.gaugeVersion)
	prometheus.MustRegister(ml.cntConnections)
	prometheus.MustRegister(ml.cntDisconnects)
	prometheus.MustRegister(ml.gaugeConcurrentConn)
	prometheus.MustRegister(ml.gaugeConcurrentQueries)
	prometheus.MustRegister(ml.histQueryDur)

	ml.gaugeVersion.Set(f64Version)
	return ml, nil
}

func (ml *metricsListener) ClientConnected() {
	ml.gaugeConcurrentConn.Add(1.0)
	ml.cntConnections.Add(1.0)
}

func (ml *metricsListener) ClientDisconnected() {
	ml.gaugeConcurrentConn.Sub(1.0)
	ml.cntDisconnects.Add(1.0)
}

func (ml *metricsListener) QueryStarted() {
	ml.gaugeConcurrentQueries.Add(1.0)
}

func (ml *metricsListener) QueryCompleted(success bool, duration time.Duration) {
	ml.gaugeConcurrentQueries.Sub(1.0)
	ml.histQueryDur.Observe(duration.Seconds())
}

func (ml *metricsListener) Close() {
	prometheus.Unregister(ml.gaugeVersion)
	prometheus.Unregister(ml.cntConnections)
	prometheus.Unregister(ml.cntDisconnects)
	prometheus.Unregister(ml.gaugeConcurrentConn)
	prometheus.Unregister(ml.gaugeConcurrentQueries)
	prometheus.Unregister(ml.histQueryDur)
}
