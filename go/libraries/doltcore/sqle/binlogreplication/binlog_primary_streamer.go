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
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

// binlogStreamer is responsible for receiving binlog events over its eventChan
// channel, and streaming those out to a connected replica over a MySQL connection.
// It also sends heartbeat events to the replica over the same connection at
// regular intervals. There is one streamer per connected replica.
type binlogStreamer struct {
	quitChan  chan struct{}
	eventChan chan []mysql.BinlogEvent
	ticker    *time.Ticker
}

// NewBinlogStreamer creates a new binlogStreamer instance.
func newBinlogStreamer() *binlogStreamer {
	return &binlogStreamer{
		quitChan:  make(chan struct{}),
		eventChan: make(chan []mysql.BinlogEvent, 5),
		ticker:    time.NewTicker(30 * time.Second),
	}
}

// startStream listens for new binlog events sent to this streamer over its binlog event
// channel and sends them over |conn|. It also listens for ticker ticks to send hearbeats
// over |conn|. The specified |binlogFormat| is used to define the format of binlog events
// and |binlogEventMeta| records the position of the stream. This method blocks until an error
// is received over the stream (e.g. the connection closing) or the streamer is closed,
// through it's quit channel.
func (streamer *binlogStreamer) startStream(ctx *sql.Context, conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	if err := sendInitialEvents(ctx, conn, binlogFormat, binlogEventMeta); err != nil {
		return err
	}

	for {
		logrus.StandardLogger().Trace("binlog streamer is listening for messages")

		select {
		case <-streamer.quitChan:
			logrus.StandardLogger().Trace("received message from streamer's quit channel")
			streamer.ticker.Stop()
			return nil

		case <-streamer.ticker.C:
			logrus.StandardLogger().Trace("sending binlog heartbeat")
			if err := sendHeartbeat(conn, binlogFormat, binlogEventMeta); err != nil {
				return err
			}
			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush binlog connection: %s", err.Error())
			}

		case events := <-streamer.eventChan:
			// TODO: If an error occurs while sending an event, it would be nice to have a retry at this
			//       level. Technically the replica should be abel to automatically reconnect and restart
			//       the stream from the last GTID it executed successfully, but it would be better to
			//       avoid the extra work for the reconnection and restart if possible.
			logrus.StandardLogger().Tracef("streaming %d binlog events", len(events))
			for _, event := range events {
				if err := conn.WriteBinlogEvent(event, false); err != nil {
					return err
				}
				binlogEventMeta.NextLogPosition += event.Length()
			}
			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush binlog connection: %s", err.Error())
			}
		}
	}
}

// binlogStreamerManager manages a collection of binlogStreamers, one for reach connected replica,
// and implements the doltdb.DatabaseUpdateListener interface to receive notifications of database
// changes that need to be turned into binlog events and then sent to connected replicas.
type binlogStreamerManager struct {
	streamers      []*binlogStreamer
	streamersMutex sync.Mutex
	quitChan       chan struct{}
}

// NewBinlogStreamerManager creates a new binlogStreamerManager instance.
func newBinlogStreamerManager() *binlogStreamerManager {
	manager := &binlogStreamerManager{
		streamers:      make([]*binlogStreamer, 0),
		streamersMutex: sync.Mutex{},
		quitChan:       make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-manager.quitChan:
				streamers := manager.copyStreamers()
				for _, streamer := range streamers {
					streamer.quitChan <- struct{}{}
				}
				return
			}
		}
	}()

	return manager
}

// copyStreamers returns a copy of the streamers owned by this streamer manager.
func (m *binlogStreamerManager) copyStreamers() []*binlogStreamer {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	results := make([]*binlogStreamer, len(m.streamers))
	copy(results, m.streamers)
	return results
}

// StartStream starts a new binlogStreamer and streams events over |conn| until the connection
// is closed, the streamer is sent a quit signal over its quit channel, or the streamer receives
// errors while sending events over the connection. Note that this method blocks until the
// streamer exits.
func (m *binlogStreamerManager) StartStream(ctx *sql.Context, conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	streamer := newBinlogStreamer()
	m.addStreamer(streamer)
	defer m.removeStreamer(streamer)

	return streamer.startStream(ctx, conn, binlogFormat, binlogEventMeta)
}

// sendEvents sends |binlogEvents| to all the streams managed by this instance.
func (m *binlogStreamerManager) sendEvents(binlogEvents []mysql.BinlogEvent) {
	for _, streamer := range m.copyStreamers() {
		logrus.StandardLogger().Tracef("queuing %d binlog events\n", len(binlogEvents))
		streamer.eventChan <- binlogEvents
	}
}

// addStreamer adds |streamer| to the slice of streamers managed by this binlogStreamerManager.
func (m *binlogStreamerManager) addStreamer(streamer *binlogStreamer) {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	m.streamers = append(m.streamers, streamer)
}

// removeStreamer removes |streamer| from the slice of streamers managed by this binlogStreamerManager.
func (m *binlogStreamerManager) removeStreamer(streamer *binlogStreamer) {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	m.streamers = make([]*binlogStreamer, len(m.streamers)-1, 0)
	for _, element := range m.streamers {
		if element != streamer {
			m.streamers = append(m.streamers, element)
		}
	}
}

func sendHeartbeat(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	binlogEventMeta.Timestamp = uint32(0) // Timestamp is zero for a heartbeat event
	logrus.WithField("log_position", binlogEventMeta.NextLogPosition).Tracef("sending heartbeat")

	binlogEvent := mysql.NewHeartbeatEventWithLogFile(*binlogFormat, binlogEventMeta, binlogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

// sendInitialEvents sends the initial binlog events (i.e. Rotate, FormatDescription) over a newly established binlog
// streaming connection.
func sendInitialEvents(_ *sql.Context, conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	err := sendRotateEvent(conn, binlogFormat, binlogEventMeta)
	if err != nil {
		return err
	}

	err = sendFormatDescription(conn, binlogFormat, binlogEventMeta)
	if err != nil {
		return err
	}

	return conn.FlushBuffer()
}

func sendRotateEvent(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	binlogFilePosition := uint64(0)
	binlogEventMeta.NextLogPosition = uint32(binlogFilePosition)

	binlogEvent := mysql.NewRotateEvent(*binlogFormat, binlogEventMeta, binlogFilePosition, binlogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendFormatDescription(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	binlogEvent := mysql.NewFormatDescriptionEvent(*binlogFormat, binlogEventMeta)
	binlogEventMeta.NextLogPosition += binlogEvent.Length()
	return conn.WriteBinlogEvent(binlogEvent, false)
}
