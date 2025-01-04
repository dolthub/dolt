// Copyright 2022 Dolthub, Inc.
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

package clusterdb

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type ReplicaStatus struct {
	// The name of the database this replica status represents.
	Database string
	// The role this server is currently running as. "primary" or "standby".
	Role string
	// The epoch of this server's current role.
	Epoch int
	// The standby remote that this replica status represents.
	Remote string
	// The current replication lag. NULL when we are a standby.
	ReplicationLag *time.Duration
	// As a standby, the last time we received a root update.
	// As a primary, the last time we pushed a root update to the standby.
	LastUpdate *time.Time
	// A string describing the last encountered error.  NULL when we are a
	// standby. NULL when our last replication attempt succeeded.
	CurrentError *string
}

type ClusterStatusProvider interface {
	GetClusterStatus() []ReplicaStatus
}

var _ sql.Table = ClusterStatusTable{}

type partition struct {
}

func (p *partition) Key() []byte {
	return []byte("FULL")
}

func NewClusterStatusTable(provider ClusterStatusProvider) sql.Table {
	return ClusterStatusTable{provider}
}

type ClusterStatusTable struct {
	provider ClusterStatusProvider
}

func (t ClusterStatusTable) Name() string {
	return StatusTableName
}

func (t ClusterStatusTable) String() string {
	return StatusTableName
}

func (t ClusterStatusTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t ClusterStatusTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter((*partition)(nil)), nil
}

func (t ClusterStatusTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	if t.provider == nil {
		return sql.RowsToRowIter(), nil
	}
	return sql.RowsToRowIter(replicaStatusesToRows(t.provider.GetClusterStatus())...), nil
}

func replicaStatusesToRows(rss []ReplicaStatus) []sql.Row {
	ret := make([]sql.Row, len(rss))
	for i, rs := range rss {
		ret[i] = replicaStatusToRow(rs)
	}
	return ret
}

func replicaStatusToRow(rs ReplicaStatus) sql.Row {
	ret := make(sql.UntypedSqlRow, 7)
	ret[0] = rs.Database
	ret[1] = rs.Remote
	ret[2] = rs.Role
	ret[3] = int64(rs.Epoch)
	if rs.ReplicationLag != nil {
		ret[4] = rs.ReplicationLag.Milliseconds()
	}
	if rs.LastUpdate != nil {
		ret[5] = *rs.LastUpdate
	}
	if rs.CurrentError != nil {
		ret[6] = *rs.CurrentError
	}
	return ret
}

func (t ClusterStatusTable) Schema() sql.Schema {
	return sql.Schema{
		{Name: "database", Type: types.Text, Source: StatusTableName, PrimaryKey: true, Nullable: false},
		{Name: "standby_remote", Type: types.Text, Source: StatusTableName, PrimaryKey: true, Nullable: false},
		{Name: "role", Type: types.Text, Source: StatusTableName, PrimaryKey: false, Nullable: false},
		{Name: "epoch", Type: types.Int64, Source: StatusTableName, PrimaryKey: false, Nullable: false},
		{Name: "replication_lag_millis", Type: types.Int64, Source: StatusTableName, PrimaryKey: false, Nullable: true},
		{Name: "last_update", Type: types.Datetime, Source: StatusTableName, PrimaryKey: false, Nullable: true},
		{Name: "current_error", Type: types.Text, Source: StatusTableName, PrimaryKey: false, Nullable: true},
	}
}
