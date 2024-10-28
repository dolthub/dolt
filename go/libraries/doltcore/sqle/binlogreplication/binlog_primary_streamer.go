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
	"io"
	"os"
	"path/filepath"
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
	quitChan       chan struct{}
	ticker         *time.Ticker
	skippingGtids  bool
	currentLogFile *os.File
}

// NewBinlogStreamer creates a new binlogStreamer instance.
func newBinlogStreamer() *binlogStreamer {
	return &binlogStreamer{
		quitChan: make(chan struct{}),
		ticker:   time.NewTicker(30 * time.Second),
	}
}

// startStream listens for new binlog events sent to this streamer over its binlog event
// channel and sends them over |conn|. It also listens for ticker ticks to send heartbeats
// over |conn|. The specified |binlogFormat| is used to define the format of binlog events
// and |binlogEventMeta| records the position of the stream. This method blocks until an error
// is received over the stream (e.g. the connection closing) or the streamer is closed,
// through it's quit channel.
func (streamer *binlogStreamer) startStream(ctx *sql.Context, conn *mysql.Conn, executedGtids mysql.GTIDSet, binlogFormat *mysql.BinlogFormat, binlogEventMeta *mysql.BinlogEventMetadata, logfile string) (err error) {
	logrus.WithField("connection_id", conn.ConnectionID).
		WithField("executed_gtids", executedGtids).
		Trace("starting binlog stream")

	streamer.currentLogFile, err = openBinlogFileForReading(logfile)
	if err != nil {
		return err
	}

	// Send a fake rotate event to let the replica know what file and position we're at
	binlogEventMeta.NextLogPosition = 4
	rotateEvent := mysql.NewFakeRotateEvent(*binlogFormat, *binlogEventMeta, filepath.Base(logfile))
	if err = conn.WriteBinlogEvent(rotateEvent, false); err != nil {
		return err
	}
	_ = conn.FlushBuffer()

	defer streamer.currentLogFile.Close()

	for {
		select {
		case <-streamer.quitChan:
			logrus.Debug("received message from streamer's quit channel")
			streamer.ticker.Stop()
			return nil

		case <-streamer.ticker.C:
			logrus.Debug("sending binlog heartbeat")
			currentLogFilename := filepath.Base(streamer.currentLogFile.Name())
			if err := sendHeartbeat(conn, binlogFormat, *binlogEventMeta, currentLogFilename); err != nil {
				return err
			}
			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush binlog connection: %s", err.Error())
			}

		default:
			logrus.Trace("checking binlog file for new events...")
			// TODO: Being able to select on new updates from the file would be nicer
			err := streamer.streamNextEvents(ctx, conn,
				*binlogFormat, binlogEventMeta, filepath.Dir(logfile), executedGtids)
			if err == io.EOF {
				logrus.Trace("End of binlog file! Pausing for new events...")
				time.Sleep(250 * time.Millisecond)
			} else if err != nil {
				return err
			}
		}
	}
}

// streamNextEvents streams up to 50 of the next events from the current binary logfile to a replica connected on
// |conn|. |executedGtids| indicates which GTIDs the connected replica has already executed. |logFileDir| indicates
// where the streamer can look for more binary log files with the current file rotates. If an error, including
// io.EOF, occurs while reading from the file, it is returned.
func (streamer *binlogStreamer) streamNextEvents(_ *sql.Context, conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogEventMeta *mysql.BinlogEventMetadata, logFileDir string, executedGtids mysql.GTIDSet) error {
	for range 50 {
		binlogEvent, err := readBinlogEventFromFile(streamer.currentLogFile)
		if err != nil {
			return err
		}

		// Update next log position in the stream so that we can send the correct position
		// when a heartbeat needs to be sent by the timer signal.
		binlogEventMeta.NextLogPosition += binlogEvent.Length()

		if binlogEvent.IsRotate() {
			bytes := binlogEvent.Bytes()
			newLogfile := string(bytes[19+8 : (len(bytes) - 4)])
			logrus.Debugf("Rotatating to new binlog file: %s", newLogfile)

			if err = streamer.currentLogFile.Close(); err != nil {
				logrus.Errorf("unable to close previous binlog file: %s", err.Error())
			}

			newLogfile = filepath.Join(logFileDir, newLogfile)
			if streamer.currentLogFile, err = openBinlogFileForReading(newLogfile); err != nil {
				return err
			}

			// Reset log position to right after the 4 byte magic number for the file type
			binlogEventMeta.NextLogPosition = 4
			continue
		}

		if binlogEvent.IsGTID() {
			gtid, _, err := binlogEvent.GTID(binlogFormat)
			if err != nil {
				return err
			}

			// If the replica has already executed this GTID, then skip it.
			if executedGtids.ContainsGTID(gtid) {
				streamer.skippingGtids = true
			} else {
				streamer.skippingGtids = false
			}
		}

		if streamer.skippingGtids {
			continue
		}

		if err := conn.WriteBinlogEvent(binlogEvent, false); err != nil {
			return err
		}
		if err := conn.FlushBuffer(); err != nil {
			return err
		}
	}

	return nil
}

// binlogStreamerManager manages a collection of binlogStreamers, one for reach connected replica,
// and implements the doltdb.DatabaseUpdateListener interface to receive notifications of database
// changes that need to be turned into binlog events and then sent to connected replicas.
type binlogStreamerManager struct {
	streamers      []*binlogStreamer
	streamersMutex sync.Mutex
	quitChan       chan struct{}
	logManager     *logManager
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
// streamer exits. Note that this function does NOT validate that the primary has the correct set
// of GTIDs available to get the replica in sync with the primary – it is expected for that
// validation to have been completed before starting a binlog stream.
func (m *binlogStreamerManager) StartStream(ctx *sql.Context, conn *mysql.Conn, executedGtids mysql.GTIDSet, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	streamer := newBinlogStreamer()
	m.addStreamer(streamer)
	defer m.removeStreamer(streamer)

	file, err := m.logManager.findLogFileForPosition(executedGtids)
	if err != nil {
		return err
	}

	return streamer.startStream(ctx, conn, executedGtids, binlogFormat, &binlogEventMeta, file)
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

	if len(m.streamers) == 0 {
		return
	}

	m.streamers = make([]*binlogStreamer, len(m.streamers)-1, 0)
	for _, element := range m.streamers {
		if element != streamer {
			m.streamers = append(m.streamers, element)
		}
	}
}

// sendHeartbeat sends a heartbeat event over |conn| using the specified |binlogFormat| and |binlogEventMeta| as well
// as |currentLogFilename| to create the event payload.
func sendHeartbeat(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata, currentLogFilename string) error {
	binlogEventMeta.Timestamp = uint32(0) // Timestamp is zero for a heartbeat event
	logrus.WithField("log_position", binlogEventMeta.NextLogPosition).Tracef("sending heartbeat")

	// MySQL 8.4 requires that we pass the binlog filename in the heartbeat; previous versions accepted
	// heartbeat events without a filename, but those cause crashes on MySQL 8.4.
	binlogEvent := mysql.NewHeartbeatEventWithLogFile(*binlogFormat, binlogEventMeta, currentLogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
}
