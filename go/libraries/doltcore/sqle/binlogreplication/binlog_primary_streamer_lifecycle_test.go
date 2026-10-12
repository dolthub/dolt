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
	"net"
	"testing"
	"testing/synctest"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"
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
