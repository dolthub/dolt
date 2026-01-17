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
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/clusterdb"
	"github.com/dolthub/dolt/go/libraries/utils/version"
)

const (
	metricsUpdateInterval = time.Second * 5

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

	// sys metrics
	cpuUsage  prometheus.Gauge
	diskUsage prometheus.Gauge
	memUsage  prometheus.Gauge

	mountPoint string

	// used in updating cluster metrics
	clusterStatus  clusterdb.ClusterStatusProvider
	mu             *sync.Mutex
	done           bool
	clusterSeenDbs map[string]struct{}
}

func newMetricsListener(labels prometheus.Labels, versionStr, storagePath string, clusterStatus clusterdb.ClusterStatusProvider) (*metricsListener, error) {
	mountPoint := ""

	if storagePath != "" {
		// Use all=true to include virtual filesystems like overlay in Docker containers.
		partitions, err := disk.Partitions(true)

		if err != nil {
			logrus.Info(fmt.Sprintf("Error getting disk partitions: %v", err))
		} else {
			bestMatchLen := 0
			for _, partition := range partitions {
				mp := partition.Mountpoint
				if mp == "" {
					continue
				}
				if strings.HasPrefix(storagePath, mp) && len(mp) > bestMatchLen {
					mountPoint = mp
					bestMatchLen = len(mp)
				}
			}

			if mountPoint == "" {
				logrus.Info(fmt.Sprintf("Could not find mount point for storage path '%s'", storagePath))
			}
		}
	}

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
		cpuUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "sys_cpu_usage",
			Help:        "The percentage of CPU used by the system",
			ConstLabels: labels,
		}),
		diskUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "sys_disk_usage",
			Help:        "The percentage of disk used by the system",
			ConstLabels: labels,
		}),
		memUsage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "sys_mem_usage",
			Help:        "The percentage of memory used by the system",
			ConstLabels: labels,
		}),
		clusterStatus:  clusterStatus,
		mu:             &sync.Mutex{},
		clusterSeenDbs: make(map[string]struct{}),
		mountPoint:     mountPoint,
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
	prometheus.MustRegister(ml.cpuUsage)
	prometheus.MustRegister(ml.diskUsage)
	prometheus.MustRegister(ml.memUsage)

	go func() {
		for ml.pollMetrics() {
			time.Sleep(metricsUpdateInterval)
		}
	}()

	ml.gaugeVersion.Set(f64Version)
	return ml, nil
}

func (ml *metricsListener) pollMetrics() bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	if ml.done {
		return false
	}

	ml.pollReplicationMetrics()
	ml.pollSysMetrics()

	return true
}

func (ml *metricsListener) pollReplicationMetrics() {
	perDbStatus := ml.clusterStatus.GetClusterStatus()
	if perDbStatus == nil {
		return
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
}

func (ml *metricsListener) pollSysMetrics() {
	percentages, err := cpu.Percent(0, false)

	if err != nil {
		logrus.Info(fmt.Sprintf("Error getting CPU usage: %v", err))
	} else if len(percentages) == 1 {
		ml.cpuUsage.Set(percentages[0])
	} else {
		logrus.Infof("Unexpected number of CPU diskUsage percentages returned: %d", len(percentages))
	}

	memStats, err := mem.VirtualMemory()

	if err != nil {
		logrus.Infof("Error getting memory usage: %v", err)
	} else {
		ml.memUsage.Set(memStats.UsedPercent)
	}

	diskUsage, err := disk.Usage(ml.mountPoint)

	if err != nil {
		logrus.Infof("Error getting disk usage for mount point '%s': %v", ml.mountPoint, err)
	} else {
		ml.diskUsage.Set(diskUsage.UsedPercent)
	}

	//logrus.Infof("Cpu: %.2f%%, Mem: %.2f%%, Disk: %.2f%%", percentages[0], memStats.UsedPercent, diskUsage.UsedPercent)
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
	ml.mu.Lock()
	defer ml.mu.Unlock()

	ml.done = true

	prometheus.Unregister(ml.gaugeVersion)
	prometheus.Unregister(ml.cntConnections)
	prometheus.Unregister(ml.cntDisconnects)
	prometheus.Unregister(ml.gaugeConcurrentConn)
	prometheus.Unregister(ml.gaugeConcurrentQueries)
	prometheus.Unregister(ml.histQueryDur)

	ml.closeReplicationMetrics()
	ml.closeSysMetrics()
}

func (ml *metricsListener) closeReplicationMetrics() {
	prometheus.Unregister(ml.replicationLagGauges)
	prometheus.Unregister(ml.isReplicaGauges)
}

func (ml *metricsListener) closeSysMetrics() {
	prometheus.Unregister(ml.cpuUsage)
	prometheus.Unregister(ml.diskUsage)
	prometheus.Unregister(ml.memUsage)
}
