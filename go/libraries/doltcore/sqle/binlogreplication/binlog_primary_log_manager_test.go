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

package binlogreplication

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
)

// TestFindLogFileForPosition checks which binlog file a replica stream starts from, including
// log files without GTID events, such as the file a server rotates to when it restarts.
func TestFindLogFileForPosition(t *testing.T) {
	format := mysql.NewMySQL56BinlogFormat()
	meta := mysql.BinlogEventMetadata{ServerID: 42}
	sid := mysql.SID{1}
	gtid := func(n int64) mysql.BinlogEvent {
		return mysql.NewMySQLGTIDEvent(format, meta, mysql.Mysql56GTID{Server: sid, Sequence: n}, false)
	}
	rotate := func(next string) mysql.BinlogEvent {
		return mysql.NewRotateEvent(format, meta, 4, next)
	}
	first, second, third := "binlog-main.000001", "binlog-main.000002", "binlog-main.000003"

	tests := []struct {
		name     string
		files    map[string][]mysql.BinlogEvent
		current  string
		executed int64 // the replica has executed GTIDs 1 through |executed|
		expected string
	}{
		{
			name:     "first GTID of the first file is unexecuted",
			files:    map[string][]mysql.BinlogEvent{first: {gtid(1), gtid(2), rotate(second)}, second: {gtid(3)}},
			current:  second,
			executed: 0,
			expected: first,
		},
		{
			name:     "first GTID of a later file is unexecuted",
			files:    map[string][]mysql.BinlogEvent{first: {gtid(1), gtid(2), rotate(second)}, second: {gtid(3)}},
			current:  second,
			executed: 1,
			expected: first,
		},
		{
			name:     "all first GTIDs are executed and the current file has GTIDs",
			files:    map[string][]mysql.BinlogEvent{first: {gtid(1), gtid(2), rotate(second)}, second: {gtid(3), gtid(4)}},
			current:  second,
			executed: 3,
			expected: second,
		},
		{
			name:     "unexecuted tail before a rotated file without GTIDs",
			files:    map[string][]mysql.BinlogEvent{first: {gtid(1), gtid(2), gtid(3), rotate(second)}, second: {}},
			current:  second,
			executed: 2,
			expected: first,
		},
		{
			name: "unexecuted tail before a file without GTIDs in the middle",
			files: map[string][]mysql.BinlogEvent{
				first:  {gtid(1), gtid(2), gtid(3), rotate(second)},
				second: {rotate(third)},
				third:  {gtid(4)},
			},
			current:  third,
			executed: 2,
			expected: first,
		},
		{
			name:     "no file has GTIDs",
			files:    map[string][]mysql.BinlogEvent{first: {}},
			current:  first,
			executed: 0,
			expected: first,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, dir := newFileBackedStreamerManager(t, format, meta, tt.files)
			m.logManager.currentBinlogFileName = tt.current
			executed := mysql.GTIDSet(mysql.Mysql56GTIDSet{})
			for n := int64(1); n <= tt.executed; n++ {
				executed = executed.AddGTID(mysql.Mysql56GTID{Server: sid, Sequence: n})
			}

			file, err := m.logManager.findLogFileForPosition(executed)
			require.NoError(t, err)
			require.Equal(t, filepath.Join(dir, tt.expected), file)
		})
	}
}

// TestBinlogStreamResumesTailBeforeRestartRotation reproduces a replica reconnecting after
// the primary restarts, before anything is written to the newly rotated log file. GTIDs at
// the end of the previous file that the replica has not executed must still be streamed,
// and before any GTID written after the restart.
func TestBinlogStreamResumesTailBeforeRestartRotation(t *testing.T) {
	format := mysql.NewMySQL56BinlogFormat()
	meta := mysql.BinlogEventMetadata{ServerID: 42}
	sid := mysql.SID{1}
	gtid := func(n int64) mysql.Mysql56GTID { return mysql.Mysql56GTID{Server: sid, Sequence: n} }
	first, second := "binlog-main.000001", "binlog-main.000002"
	previous := mysql.Mysql56GTIDSet{}.AddGTID(gtid(1)).AddGTID(gtid(2)).AddGTID(gtid(3))
	m, dir := newFileBackedStreamerManager(t, format, meta, map[string][]mysql.BinlogEvent{
		first: {
			mysql.NewPreviousGtidsEvent(format, meta, mysql.Mysql56GTIDSet{}),
			mysql.NewMySQLGTIDEvent(format, meta, gtid(1), false),
			mysql.NewMySQLGTIDEvent(format, meta, gtid(2), false),
			mysql.NewMySQLGTIDEvent(format, meta, gtid(3), false),
			mysql.NewRotateEvent(format, meta, 4, second),
		},
		// The file created on restart holds only the Previous GTIDs event when the replica connects.
		second: {mysql.NewPreviousGtidsEvent(format, meta, previous.(mysql.Mysql56GTIDSet))},
	})
	m.logManager.currentBinlogFileName = second

	// The replica executed GTIDs 1-2; GTID 3 is the unexecuted tail of the previous file.
	executed := mysql.Mysql56GTIDSet{}.AddGTID(gtid(1)).AddGTID(gtid(2))
	server, client := binlogStreamTCPPair(t)
	result := make(chan error, 1)
	go func() {
		result <- m.StartStream(sql.NewEmptyContext(), &mysql.Conn{Conn: server}, executed, &format, meta)
	}()

	// One mysql.Conn for the whole stream keeps its packet sequence numbers in step.
	replica := &mysql.Conn{Conn: client}
	require.Equal(t, gtid(3), readNextStreamGTID(t, replica, format))
	appendBinlogEvents(t, filepath.Join(dir, second), mysql.NewMySQLGTIDEvent(format, meta, gtid(4), false))
	require.Equal(t, gtid(4), readNextStreamGTID(t, replica, format))

	require.NoError(t, client.SetLinger(0))
	require.NoError(t, client.Close())
	appendBinlogEvents(t, filepath.Join(dir, second), mysql.NewQueryEvent(format, meta, mysql.Query{SQL: "SELECT 1"}))
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected replica's stream did not exit")
	}
}

// readNextStreamGTID consumes binlog packets until the next GTID event and returns its GTID.
func readNextStreamGTID(t *testing.T, client *mysql.Conn, format mysql.BinlogFormat) mysql.GTID {
	t.Helper()
	require.NoError(t, client.Conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		packet, err := client.ReadPacket(context.Background())
		require.NoError(t, err)
		require.NotEmpty(t, packet)
		require.Equal(t, byte(0), packet[0], "binlog packet must have an OK prefix")
		event := mysql.NewMysql56BinlogEvent(packet[1:])
		if event.IsGTID() {
			gtid, _, err := event.GTID(format)
			require.NoError(t, err)
			return gtid
		}
	}
}
