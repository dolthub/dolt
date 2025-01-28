// Copyright 2023 Dolthub, Inc.
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
	"sync"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

// binlogEventProducer is responsible for reading binlog events from an established connection and sending them back to
// a consumer over a channel. This is necessary because calls to conn.ReadBinlogEvent() block until a binlog event is
// received. If the source isn't sending more events, then the applier is blocked on reading events, and the user
// can't issue a call to STOP REPLICA. Reading binlog events in a thread and communicating with the applier via
// channels for events and errors decouples this.
type binlogEventProducer struct {
	conn      *mysql.Conn
	errorChan chan error
	eventChan chan mysql.BinlogEvent
	closeChan chan struct{}
	wg        sync.WaitGroup
	running   atomic.Bool
}

// newBinlogEventProducer creates a new binlog event producer that reads from the specified, established MySQL
// connection |conn|. The returned binlogEventProducer owns the communication channels
// and is responsible for closing them when the binlogEventProducer is stopped.
//
// The BinlogEventProducer will take ownership of the supplied |*Conn| instance and
// will |Close| it when the producer itself exits.
func newBinlogEventProducer(conn *mysql.Conn) *binlogEventProducer {
	producer := &binlogEventProducer{
		conn:      conn,
		eventChan: make(chan mysql.BinlogEvent),
		errorChan: make(chan error),
		closeChan: make(chan struct{}),
	}
	return producer
}

// EventChan returns the event channel through which this event
// producer sends binlog events.
func (p *binlogEventProducer) EventChan() <-chan mysql.BinlogEvent {
	return p.eventChan
}

// ErrorChan returns the error channel through which this event
// producer sends any errors.
func (p *binlogEventProducer) ErrorChan() <-chan error {
	return p.errorChan
}

// Go starts this binlogEventProducer in a new goroutine. Right before this routine exits, it will close the
// two communication channels it owns.
func (p *binlogEventProducer) Go(_ *sql.Context) {
	if !p.running.CompareAndSwap(false, true) {
		panic("attempt to start binlogEventProducer more than once.")
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.errorChan)
		defer close(p.eventChan)
		for p.IsRunning() {
			// ReadBinlogEvent blocks until a binlog event can be read and returned, so this has to be done on a
			// separate thread, otherwise the applier would be blocked and wouldn't be able to handle the STOP
			// REPLICA signal.
			event, err := p.conn.ReadBinlogEvent()

			// If this binlogEventProducer has been stopped while we were blocked waiting to read the next
			// binlog event, abort processing it and just return instead.
			if p.IsRunning() == false {
				break
			}

			if err != nil {
				select {
				case p.errorChan <- err:
				case <-p.closeChan:
					return
				}
			} else {
				select {
				case p.eventChan <- event:
				case <-p.closeChan:
					return
				}
			}
		}
	}()
}

// IsRunning returns true if this instance is processing binlog events and has not been stopped.
func (p *binlogEventProducer) IsRunning() bool {
	return p.running.Load()
}

// Stop requests for this binlogEventProducer to stop processing events as soon as possible.
func (p *binlogEventProducer) Stop() {
	if p.running.CompareAndSwap(true, false) {
		p.conn.Close()
		close(p.closeChan)
	}
	p.wg.Wait()
}
