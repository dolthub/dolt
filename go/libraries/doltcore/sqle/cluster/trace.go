// Copyright 2026 Dolthub, Inc.
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

package cluster

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// This file holds temporary diagnostics for a rare, CI-only race in graceful
// role transitions: "could not replicate databases to standby in a timely
// manner" despite the standby being nominally up. Every message it emits (and
// every message emitted by callers of tsNow) contains the token
// "cluster/trace" so the relevant lines can be extracted from a noisy server
// log with a single grep. The default logrus text formatter only has
// one-second timestamp granularity, which is useless for sequencing events
// inside a ten-second window, so these messages embed their own
// microsecond-resolution wallclock via tsNow.

// tsNow returns the current UTC wallclock with microsecond resolution, for
// embedding directly in trace messages.
func tsNow() string {
	return time.Now().UTC().Format("15:04:05.000000")
}

// connStateStr describes the current connectivity state of a grpc channel,
// tolerating the nil channels that show up in unit tests.
func connStateStr(conn *grpc.ClientConn) string {
	if conn == nil {
		return "<nil-conn>"
	}
	return conn.GetState().String()
}

// watchConnState logs every connectivity state transition of |conn| until the
// channel shuts down. The channel's internal state machine (IDLE, CONNECTING,
// TRANSIENT_FAILURE, READY) and in particular *when* it moves between those
// states is the ground truth for whether ResetConnectBackoff calls are
// actually producing dial attempts, which is not observable from RPC errors
// alone.
func watchConnState(lgr *logrus.Entry, remote string, conn *grpc.ClientConn) {
	go func() {
		state := conn.GetState()
		lgr.Tracef("cluster/trace[grpcconn %s]: ts=%s initial channel state %s", remote, tsNow(), state)
		for state != connectivity.Shutdown {
			if !conn.WaitForStateChange(context.Background(), state) {
				return
			}
			newState := conn.GetState()
			lgr.Tracef("cluster/trace[grpcconn %s]: ts=%s channel state %s -> %s", remote, tsNow(), state, newState)
			state = newState
		}
	}()
}
