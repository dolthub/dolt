// Copyright 2024 Dolthub, Inc.
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

package binlogreplication

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

// binlogBranch specifies the branch used for generating binlog events.
const binlogBranch = "main"

// binlogFilename is the name of the filename used in binlog events. Note that
// currently, this doesn't map to a real file on disk yet, but the filename is
// still needed for binlog messages.
const binlogFilename = "binlog-" + binlogBranch + ".000001"

// BinlogEnabled indicates whether binary logging is enabled or not. Similar to Dolt's other replication features,
// changes to binary logging are only applied at server startup.
//
// NOTE: By default, binary logging for Dolt is not enabled, which differs from MySQL's @@log_bin default. Dolt's
// binary logging is initially an opt-in feature, but we may change that after measuring and tuning the
// performance hit that binary logging adds.
var BinlogEnabled = false

// DoltBinlogPrimaryController is a singleton instance of the doltBinlogPrimaryController struct
// and is registered with the SQL engine to handle protocol commands and SQL statements that
// are related to a binlog primary server (e.g. COM_DUMP_GTID, `SHOW BINARY LOG STATUS`).
var DoltBinlogPrimaryController = newDoltBinlogPrimaryController()

type registeredReplica struct {
	connectionId uint32
	host         string
	port         uint16
}

// doltBinlogPrimaryController implements the binlogreplication.BinlogPrimaryController
// interface from GMS and is the main extension point where Dolt plugs in to GMS and
// interprets commands and statements related to serving binlog events.
type doltBinlogPrimaryController struct {
	registeredReplicas []*registeredReplica
}

var _ binlogreplication.BinlogPrimaryController = (*doltBinlogPrimaryController)(nil)

// newDoltBinlogPrimaryController creates a new doltBinlogPrimaryController instance.
func newDoltBinlogPrimaryController() *doltBinlogPrimaryController {
	controller := doltBinlogPrimaryController{
		registeredReplicas: make([]*registeredReplica, 0),
	}
	return &controller
}

// RegisterReplica implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) RegisterReplica(ctx *sql.Context, c *mysql.Conn, replicaHost string, replicaPort uint16) error {
	// TODO: Do we actually need the connection here? Doesn't seem like it...
	// TODO: Obviously need locking on the datastructure, but just getting something stubbed out
	d.registeredReplicas = append(d.registeredReplicas, &registeredReplica{
		connectionId: c.ConnectionID,
		host:         replicaHost,
		port:         replicaPort,
	})

	return nil
}

// BinlogDumpGtid implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) BinlogDumpGtid(ctx *sql.Context, conn *mysql.Conn, gtidSet mysql.GTIDSet) error {
	err := doltBinlogStreamerManager.StreamEvents(ctx, conn)
	if err != nil {
		logrus.Warnf("exiting binlog streamer due to error: %s", err.Error())
	} else {
		logrus.Trace("exiting binlog streamer cleanly")
	}

	return err
}

// ListReplicas implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) ListReplicas(ctx *sql.Context) error {
	return fmt.Errorf("ListReplicas not implemented in Dolt yet")
}

// ListBinaryLogs implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) ListBinaryLogs(ctx *sql.Context) error {
	return fmt.Errorf("ListBinaryLogs not implemented in Dolt yet")
}

// GetBinaryLogStatus implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) GetBinaryLogStatus(ctx *sql.Context) ([]binlogreplication.BinaryLogStatus, error) {
	serverUuid, err := getServerUuid(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: This data is just stubbed out; need to fill in the correct GTID info
	return []binlogreplication.BinaryLogStatus{{
		File:          binlogFilename,
		Position:      uint(doltBinlogStreamerManager.binlogStream.LogPosition),
		DoDbs:         "",
		IgnoreDbs:     "",
		ExecutedGtids: serverUuid + ":1-3",
	}}, nil
}
