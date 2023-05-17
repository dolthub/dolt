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
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/clusterdb"
	"github.com/dolthub/dolt/go/libraries/utils/version"
)

const (
	clusterUpdateInterval = time.Second * 5

	dbLabel     = "database"
	roleLabel   = "role"
	remoteLabel = "remote"
)

var _ server.ServerEventListener = (*metricsListener)(nil)

type metricsListener struct {
	labels prometheus.Labels

	cntConnections         prometheus.Counter
	cntDisconnects         prometheus.Counter
	gaugeConcurrentConn    prometheus.Gauge
	gaugeConcurrentQueries prometheus.Gauge
	histQueryDur           prometheus.Histogram
	gaugeVersion           prometheus.Gauge

	// replication metrics
	isReplicaGauges      *prometheus.GaugeVec
	replicationLagGauges *prometheus.GaugeVec

	// used in updating cluster metrics
	clusterStatus  clusterdb.ClusterStatusProvider
	mu             *sync.Mutex
	done           bool
	clusterSeenDbs map[string]struct{}
}

func newMetricsListener(labels prometheus.Labels, versionStr string, clusterStatus clusterdb.ClusterStatusProvider) (*metricsListener, error) {
	ml := &metricsListener{
		labels: labels,
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
		replicationLagGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "dss_replication_lag",
			Help:        "The reported replication lag of this server when it is a primary to the given standby.",
			ConstLabels: labels,
		}, []string{dbLabel, remoteLabel}),
		isReplicaGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "dss_is_replica",
			Help:        "one if the server is currently in this role, zero otherwise",
			ConstLabels: labels,
		}, []string{dbLabel}),
		clusterStatus:  clusterStatus,
		mu:             &sync.Mutex{},
		clusterSeenDbs: make(map[string]struct{}),
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
	prometheus.MustRegister(ml.replicationLagGauges)
	prometheus.MustRegister(ml.isReplicaGauges)

	go func() {
		for ml.updateReplMetrics() {
			time.Sleep(clusterUpdateInterval)
		}
	}()

	ml.gaugeVersion.Set(f64Version)
	return ml, nil
}

func (ml *metricsListener) updateReplMetrics() bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.done {
		return false
	}

	perDbStatus := ml.clusterStatus.GetClusterStatus()
	if perDbStatus == nil {
		return true
	}

	dbNames := make(map[string]struct{})
	for _, status := range perDbStatus {
		dbName := status.Database
		dbNames[dbName] = struct{}{}

		if status.Role == string(cluster.RolePrimary) {
			ml.isReplicaGauges.WithLabelValues(status.Database).Set(0.0)

			if status.ReplicationLag == nil {
				ml.replicationLagGauges.WithLabelValues(status.Database, status.Remote).Set(-1.0)
			} else {
				ml.replicationLagGauges.WithLabelValues(status.Database, status.Remote).Set(float64(status.ReplicationLag.Milliseconds()))
			}
		} else {
			ml.isReplicaGauges.WithLabelValues(status.Database).Set(1.0)
			ml.replicationLagGauges.WithLabelValues(status.Database, status.Remote).Set(-1.0)
		}
	}

	// deregister metrics for deleted databases
	for db := range ml.clusterSeenDbs {
		if _, ok := dbNames[db]; !ok {
			ml.isReplicaGauges.DeletePartialMatch(prometheus.Labels{"database": db})
			ml.replicationLagGauges.DeletePartialMatch(prometheus.Labels{"database": db})
		}
	}
	ml.clusterSeenDbs = dbNames

	return true
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

	ml.closeReplicationMetrics()
}

func (ml *metricsListener) closeReplicationMetrics() {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	prometheus.Unregister(ml.replicationLagGauges)
	prometheus.Unregister(ml.isReplicaGauges)

	ml.done = true
}
