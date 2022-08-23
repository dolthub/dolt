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
	"strconv"
	"strings"
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

	version, err := encodeVersion(versionStr)
	if err != nil {
		return nil, err
	}

	prometheus.MustRegister(ml.gaugeVersion)
	prometheus.MustRegister(ml.cntConnections)
	prometheus.MustRegister(ml.cntDisconnects)
	prometheus.MustRegister(ml.gaugeConcurrentConn)
	prometheus.MustRegister(ml.gaugeConcurrentQueries)
	prometheus.MustRegister(ml.histQueryDur)

	ml.gaugeVersion.Set(version)

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
	prometheus.Unregister(ml.cntConnections)
	prometheus.Unregister(ml.cntDisconnects)
	prometheus.Unregister(ml.gaugeConcurrentConn)
	prometheus.Unregister(ml.gaugeConcurrentQueries)
	prometheus.Unregister(ml.histQueryDur)
	prometheus.Unregister(ml.gaugeVersion)
}

func encodeVersion(version string) (float64, error) {
	parts := strings.Split(version, ".")

	if len(parts) != 3 {
		return 0, fmt.Errorf("version '%s' is not in the format X.X.X", version)
	}

	partVals := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		var err error
		partVals[i], err = strconv.ParseUint(parts[i], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("failed to parse version '%s'. error at '%s': %w", version, parts[i], err)
		}
	}

	if partVals[0] > 255 || partVals[1] > 255 || partVals[2] > 65535 {
		return 0, fmt.Errorf("version '%s' cannot be encoded with 8 bits for major, 8 bits for minor, 16 bits for build", version)
	}

	versionUint32 := (uint32(partVals[0]&0xFF) << 24) | (uint32(partVals[1]&0xFF) << 16) | uint32(partVals[2]&0xFFFF)
	return float64(versionUint32), nil
}

func decodeVersion(version float64) string {
	versInt32 := uint32(version)
	major := (versInt32 & 0xFF000000) >> 24
	minor := (versInt32 & 0x00FF0000) >> 16
	build := versInt32 & 0x0000FFFF

	majorStr := strconv.FormatUint(uint64(major), 10)
	minorStr := strconv.FormatUint(uint64(minor), 10)
	buildStr := strconv.FormatUint(uint64(build), 10)

	return majorStr + "." + minorStr + "." + buildStr
}
