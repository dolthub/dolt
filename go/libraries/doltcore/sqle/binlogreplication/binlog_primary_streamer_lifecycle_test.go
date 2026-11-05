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
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

func TestBinlogStreamerManagerClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newBinlogStreamerManager()
		m.Close()
		m.Close()
		// A rejected stream must return before consulting an unconfigured log manager.
		require.ErrorContains(t, m.StartStream(nil, nil, nil, nil, mysql.BinlogEventMetadata{}), "manager is closed")
		synctest.Wait()
	})
}

func TestBinlogStreamerManagerCloseInterruptsStreams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newBinlogStreamerManager()
		writer, reader := net.Pipe()
		defer reader.Close()
		blocked := newBinlogStreamer()
		defer blocked.ticker.Stop()
		blocked.conn = &mysql.Conn{Conn: writer}
		require.True(t, m.addStreamer(blocked))
		writeErr := make(chan error, 1)
		go func() {
			defer m.removeStreamer(blocked)
			_, err := writer.Write([]byte("binlog event"))
			writeErr <- err
		}()

		idleWriter, idleReader := net.Pipe()
		defer idleReader.Close()
		idle := newBinlogStreamer()
		defer idle.ticker.Stop()
		idle.conn = &mysql.Conn{Conn: idleWriter}
		require.True(t, m.addStreamer(idle))
		go func() {
			defer m.removeStreamer(idle)
			<-idle.quitChan
		}()
		synctest.Wait()
		m.Close()
		require.Error(t, <-writeErr)
		require.Empty(t, m.copyStreamers())
		m.Close()
	})
}

func TestBinlogStreamerManagerRemovePreservesOtherStreams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newBinlogStreamerManager()
		streamers := make([]*binlogStreamer, 3)
		for i := range streamers {
			streamers[i] = newBinlogStreamer()
			defer streamers[i].ticker.Stop()
			require.True(t, m.addStreamer(streamers[i]))
		}
		m.removeStreamer(streamers[1])
		require.Equal(t, []*binlogStreamer{streamers[0], streamers[2]}, m.copyStreamers())
		m.removeStreamer(streamers[0])
		m.removeStreamer(streamers[2])
		m.Close()
	})
}

func TestBinlogStreamerManagerConcurrentCloseAndRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newBinlogStreamerManager()
		go m.Close()
		for range 20 {
			writer, reader := net.Pipe()
			defer reader.Close()
			s := newBinlogStreamer()
			defer s.ticker.Stop()
			s.conn = &mysql.Conn{Conn: writer}
			if m.addStreamer(s) {
				go func() {
					defer m.removeStreamer(s)
					<-s.quitChan
				}()
			} else {
				writer.Close()
			}
		}
		m.Close()
		synctest.Wait()
		require.Empty(t, m.copyStreamers())
	})
}

// TestBinlogStreamDisconnectAfterRotation checks descriptor cleanup in the live primary
// process, where process exit cannot conceal a leaked file.
func TestBinlogStreamDisconnectAfterRotation(t *testing.T) {
	format := mysql.NewMySQL56BinlogFormat()
	meta := mysql.BinlogEventMetadata{ServerID: 42}
	sid := mysql.SID{1}
	first := "binlog-main.000001"
	second := "binlog-main.000002"
	m, dir := newFileBackedStreamerManager(t, format, meta, map[string][]mysql.BinlogEvent{
		first: {
			mysql.NewMySQLGTIDEvent(format, meta, mysql.Mysql56GTID{Server: sid, Sequence: 1}, false),
			mysql.NewRotateEvent(format, meta, 4, second),
		},
		second: {mysql.NewMySQLGTIDEvent(format, meta, mysql.Mysql56GTID{Server: sid, Sequence: 2}, false)},
	})
	server, client := binlogStreamTCPPair(t)
	result := make(chan error, 1)
	go func() {
		result <- m.StartStream(sql.NewEmptyContext(), &mysql.Conn{Conn: server}, mysql.Mysql56GTIDSet{}, &format, meta)
	}()
	readStreamUntilGTID(t, client, format, mysql.Mysql56GTID{Server: sid, Sequence: 2})
	streamers := m.copyStreamers()
	require.Len(t, streamers, 1)

	// Force the next event write to observe the disconnect, rather than waiting
	// for the thirty-second heartbeat interval.
	require.NoError(t, client.SetLinger(0))
	require.NoError(t, client.Close())
	appendBinlogEvents(t, filepath.Join(dir, second), mysql.NewQueryEvent(format, meta, mysql.Query{SQL: "SELECT 1"}))
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected replica's stream did not exit")
	}

	// Reading the field only after StartStream returns avoids racing rotation.
	require.Equal(t, filepath.Join(dir, second), streamers[0].currentLogFile.Name())
	_, err := streamers[0].currentLogFile.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.Empty(t, m.copyStreamers())
	m.Close()
	require.ErrorContains(t, m.StartStream(nil, nil, nil, nil, meta), "manager is closed")
}

// TestBinlogStreamCloseInterruptsBlockedReplica exercises shutdown while the actual
// streaming path is blocked writing a binlog event to a TCP replica that stops reading.
func TestBinlogStreamCloseInterruptsBlockedReplica(t *testing.T) {
	format := mysql.NewMySQL56BinlogFormat()
	meta := mysql.BinlogEventMetadata{ServerID: 42}
	m, _ := newFileBackedStreamerManager(t, format, meta, map[string][]mysql.BinlogEvent{
		"binlog-main.000001": {mysql.NewQueryEvent(format, meta, mysql.Query{SQL: strings.Repeat("x", 4<<20)})},
	})
	server, client := binlogStreamTCPPair(t)
	require.NoError(t, server.SetWriteBuffer(1024))
	require.NoError(t, client.SetReadBuffer(1024))
	writeStarted := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		conn := &mysql.Conn{Conn: &binlogWriteObserver{Conn: server, started: writeStarted}}
		result <- m.StartStream(sql.NewEmptyContext(), conn, mysql.Mysql56GTIDSet{}, &format, meta)
	}()
	select {
	case <-writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not start writing the large event")
	}
	select {
	case err := <-result:
		t.Fatalf("stream exited before shutdown: %v", err)
	case <-time.After(100 * time.Millisecond):
		// The unread event exceeds both TCP buffers and its write remains blocked.
	}
	streamers := m.copyStreamers()
	require.Len(t, streamers, 1)
	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("primary shutdown did not interrupt the blocked binlog write")
	}
	require.Error(t, <-result)
	_, err := streamers[0].currentLogFile.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.Empty(t, m.copyStreamers())
	m.Close()
}

// binlogWriteObserver signals entry into the large event's real TCP write.
type binlogWriteObserver struct {
	net.Conn
	started chan struct{}
}

// Write observes large writes without changing their blocking or error behavior.
func (c *binlogWriteObserver) Write(data []byte) (int, error) {
	if len(data) > 1<<20 {
		select {
		case c.started <- struct{}{}:
		default:
		}
	}
	return c.Conn.Write(data)
}

// newFileBackedStreamerManager supplies real binlog files to the production stream
// manager without starting a subprocess, so file and goroutine cleanup remain observable.
func newFileBackedStreamerManager(t *testing.T, format mysql.BinlogFormat, meta mysql.BinlogEventMetadata, events map[string][]mysql.BinlogEvent) (*binlogStreamerManager, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, binlogDirectory)
	require.NoError(t, os.MkdirAll(dir, 0755))
	for name, contents := range events {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, binlogFileMagicNumber, 0644))
		appendBinlogEvents(t, path, mysql.NewFormatDescriptionEvent(format, meta))
		appendBinlogEvents(t, path, contents...)
	}
	fs, err := filesys.LocalFilesysWithWorkingDir(root)
	require.NoError(t, err)
	m := newBinlogStreamerManager()
	m.logManager = &logManager{
		fs:                    fs,
		binlogDirectory:       dir,
		binlogFormat:          format,
		currentBinlogFileName: "binlog-main.000001",
	}
	t.Cleanup(m.Close)
	return m, dir
}

// appendBinlogEvents appends serialized production events to a binlog fixture.
func appendBinlogEvents(t *testing.T, path string, events ...mysql.BinlogEvent) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer file.Close()
	for _, event := range events {
		_, err = file.Write(event.Bytes())
		require.NoError(t, err)
	}
}

// binlogStreamTCPPair returns the two ends of a real loopback TCP connection.
func binlogStreamTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer listener.Close()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	require.NoError(t, err)
	server, err := listener.AcceptTCP()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return server, client
}

// readStreamUntilGTID consumes real MySQL binlog packets through the requested GTID.
func readStreamUntilGTID(t *testing.T, conn *net.TCPConn, format mysql.BinlogFormat, expected mysql.Mysql56GTID) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	client := &mysql.Conn{Conn: conn}
	for {
		packet, err := client.ReadPacket(context.Background())
		require.NoError(t, err)
		require.NotEmpty(t, packet)
		require.Equal(t, byte(0), packet[0], "binlog packet must have an OK prefix")
		event := mysql.NewMysql56BinlogEvent(packet[1:])
		if event.IsGTID() {
			gtid, _, err := event.GTID(format)
			require.NoError(t, err)
			if gtid == expected {
				return
			}
		}
	}
}
