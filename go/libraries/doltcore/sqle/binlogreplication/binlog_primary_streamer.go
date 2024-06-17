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
	"encoding/binary"
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
	quitChan chan struct{}
	ticker   *time.Ticker
}

// NewBinlogStreamer creates a new binlogStreamer instance.
func newBinlogStreamer() *binlogStreamer {
	return &binlogStreamer{
		quitChan: make(chan struct{}),
		ticker:   time.NewTicker(30 * time.Second),
	}
}

// startStream listens for new binlog events sent to this streamer over its binlog event
// channel and sends them over |conn|. It also listens for ticker ticks to send hearbeats
// over |conn|. The specified |binlogFormat| is used to define the format of binlog events
// and |binlogEventMeta| records the position of the stream. This method blocks until an error
// is received over the stream (e.g. the connection closing) or the streamer is closed,
// through it's quit channel.
func (streamer *binlogStreamer) startStream(_ *sql.Context, conn *mysql.Conn, executedGtids mysql.GTIDSet, binlogFormat *mysql.BinlogFormat, binlogEventMeta *mysql.BinlogEventMetadata, logfile string) error {
	logrus.Tracef("Starting binlog stream... (connection ID: %d)", conn.ConnectionID)

	if err := sendInitialEvents(ctx, conn, binlogFormat, binlogEventMeta); err != nil {
		return err
	}

	// TODO: Maybe we should just ask the LogManager to give us the file for reading?
	file, err := openBinlogFileForReading(logfile)
	if err != nil {
		return err
	}

	binlogEventMetaCopy := binlogEventMeta
	binlogEventMetaCopy.NextLogPosition = 0

	rotateEvent := mysql.NewFakeRotateEvent(*binlogFormat, binlogEventMetaCopy, filepath.Base(logfile))
	if err = conn.WriteBinlogEvent(rotateEvent, false); err != nil {
		return err
	}
	_ = conn.FlushBuffer()

	defer file.Close()

	for {
		logrus.Trace("binlog streamer is listening for messages")

		select {
		case <-streamer.quitChan:
			logrus.Trace("received message from streamer's quit channel")
			streamer.ticker.Stop()
			return nil

		case <-streamer.ticker.C:
			logrus.Trace("sending binlog heartbeat")
			if err := sendHeartbeat(conn, binlogFormat, *binlogEventMeta); err != nil {
				return err
			}
			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush binlog connection: %s", err.Error())
			}

		default:
			logrus.Debug("checking file for new data...")
			eof := false
			skippingGtids := false
			for !eof {
				headerBuffer := make([]byte, 4+1+4+4+4+2)
				bytesRead, err := file.Read(headerBuffer)
				if err != nil && err != io.EOF {
					return err
				}
				if err == io.EOF {
					logrus.Tracef("End of binlog file! Waiting for new events...")
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Event Header:
				//timestamp := headerBuffer[0:4]
				//eventType := headerBuffer[4]
				//serverId := binary.LittleEndian.Uint32(headerBuffer[5:5+4])
				eventSize := binary.LittleEndian.Uint32(headerBuffer[9 : 9+4])

				payloadBuffer := make([]byte, eventSize-uint32(len(headerBuffer)))
				bytesRead, err = file.Read(payloadBuffer)
				if err != nil && err != io.EOF {
					return err
				}
				if err == io.EOF {
					logrus.Tracef("End of binlog file! Waiting for new events...")
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if bytesRead > 0 {
					binlogEvent := mysql.NewMysql56BinlogEvent(append(headerBuffer, payloadBuffer...))

					if binlogEvent.IsRotate() {
						newLogfile := string(payloadBuffer[8:(len(payloadBuffer) - 4)])
						logrus.Errorf("Rotatating to new binlog file: %s", newLogfile)

						dir := filepath.Dir(logfile)
						newLogfile = filepath.Join(dir, newLogfile)

						if err = file.Close(); err != nil {
							logrus.Errorf("unable to close previous binlog file: %s", err.Error())
						}

						if file, err = openBinlogFileForReading(newLogfile); err != nil {
							return err
						}

						continue
					}

					if binlogEvent.IsGTID() {
						gtid, _, err := binlogEvent.GTID(*binlogFormat)
						if err != nil {
							return err
						}

						// If the replica has already executed this GTID, then skip it.
						if executedGtids.ContainsGTID(gtid) {
							skippingGtids = true
						} else {
							skippingGtids = false
						}
					}

					if skippingGtids {
						continue
					}

					if err := conn.WriteBinlogEvent(binlogEvent, false); err != nil {
						return err
					}
					if err = conn.FlushBuffer(); err != nil {
						return err
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// openBinlogFileForReading opens the specified |logfile| for reading and reads the first four bytes to make sure they
// are the expected binlog file magic numbers. If any problems are encountered opening the file or reading the first
// four bytes, an error is returned.
func openBinlogFileForReading(logfile string) (*os.File, error) {
	logrus.Errorf("Opening binlog file: %s", logfile)

	file, err := os.Open(logfile)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, len(binlogFileMagicNumber))
	bytesRead, err := file.Read(buffer)
	if err != nil {
		return nil, err
	}
	if bytesRead != len(binlogFileMagicNumber) || string(buffer) != string(binlogFileMagicNumber) {
		return nil, fmt.Errorf("invalid magic number in binlog file!")
	}

	return file, nil
}

// binlogStreamerManager manages a collection of binlogStreamers, one for reach connected replica,
// and implements the doltdb.DatabaseUpdateListener interface to receive notifications of database
// changes that need to be turned into binlog events and then sent to connected replicas.
type binlogStreamerManager struct {
	streamers      []*binlogStreamer
	streamersMutex sync.Mutex
	quitChan       chan struct{}
	logManager     *LogManager
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
func (m *binlogStreamerManager) StartStream(ctx *sql.Context, conn *mysql.Conn, executedGtids mysql.GTIDSet, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	streamer := newBinlogStreamer()
	m.addStreamer(streamer)
	defer m.removeStreamer(streamer)

	return streamer.startStream(ctx, conn, executedGtids, binlogFormat, &binlogEventMeta, m.logManager.currentBinlogFilepath())
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

// LogManager sets the LogManager this streamer manager will work with to find
// and read from binlog files.
func (m *binlogStreamerManager) LogManager(manager *LogManager) {
	m.logManager = manager
}

func sendHeartbeat(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta mysql.BinlogEventMetadata) error {
	binlogEventMeta.Timestamp = uint32(0) // Timestamp is zero for a heartbeat event
	logrus.WithField("log_position", binlogEventMeta.NextLogPosition).Tracef("sending heartbeat")

	binlogEvent := mysql.NewHeartbeatEvent(*binlogFormat, binlogEventMeta)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

// sendInitialEvents sends the initial binlog events (i.e. Rotate, FormatDescription) over a newly established binlog
// streaming connection.
func sendInitialEvents(_ *sql.Context, conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta *mysql.BinlogEventMetadata) error {
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

func sendRotateEvent(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta *mysql.BinlogEventMetadata) error {
	binlogFilePosition := uint64(0)
	binlogEventMeta.NextLogPosition = uint32(binlogFilePosition)

	// The Rotate event sent at the start of a stream is a "virtual" event that isn't actually
	// recorded to the binary log file, but sent to the replica so it knows what file is being
	// read from. Because it is virtual, we do NOT update the nextLogPosition field of
	// BinlogEventMetadata.
	binlogEvent := mysql.NewRotateEvent(*binlogFormat, *binlogEventMeta, binlogFilePosition, binlogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendFormatDescription(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogEventMeta *mysql.BinlogEventMetadata) error {
	binlogEvent := mysql.NewFormatDescriptionEvent(*binlogFormat, *binlogEventMeta)
	binlogEventMeta.NextLogPosition += binlogEvent.Length()
	return conn.WriteBinlogEvent(binlogEvent, false)
}
