package multiplex

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	mpool "gx/ipfs/QmRQhVisS8dmPbjBUthVkenn81pBxrx1GxE281csJhm2vL/go-msgio/mpool"
	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
)

var log = logging.Logger("multiplex")

var MaxMessageSize = 1 << 20

var ErrShutdown = errors.New("session shut down")

const (
	NewStream = iota
	Receiver
	Initiator
	Close
)

type Multiplex struct {
	con       net.Conn
	buf       *bufio.Reader
	nextID    uint64
	initiator bool

	closed       chan struct{}
	shutdown     chan struct{}
	shutdownErr  error
	shutdownLock sync.Mutex

	wrLock sync.Mutex

	nstreams chan *Stream
	hdrBuf   []byte

	channels map[uint64]*Stream
	chLock   sync.Mutex
}

func NewMultiplex(con net.Conn, initiator bool) *Multiplex {
	mp := &Multiplex{
		con:       con,
		initiator: initiator,
		buf:       bufio.NewReader(con),
		channels:  make(map[uint64]*Stream),
		closed:    make(chan struct{}),
		shutdown:  make(chan struct{}),
		nstreams:  make(chan *Stream, 16),
		hdrBuf:    make([]byte, 20),
	}

	go mp.handleIncoming()

	return mp
}

func (mp *Multiplex) newStream(id uint64, name string, initiator bool) *Stream {
	var hfn uint64
	if initiator {
		hfn = Initiator
	} else {
		hfn = Receiver
	}
	return &Stream{
		id:     id,
		name:   name,
		header: (id << 3) | hfn,
		dataIn: make(chan []byte, 8),
		mp:     mp,
	}
}

func (m *Multiplex) Accept() (*Stream, error) {
	select {
	case s, ok := <-m.nstreams:
		if !ok {
			return nil, errors.New("multiplex closed")
		}
		return s, nil
	case <-m.closed:
		return nil, m.shutdownErr
	}
}

func (mp *Multiplex) Close() error {
	mp.closeNoWait()

	// Wait for the receive loop to finish.
	<-mp.closed

	return nil
}

func (mp *Multiplex) closeNoWait() {
	mp.shutdownLock.Lock()
	select {
	case <-mp.shutdown:
	default:
		mp.con.Close()
		close(mp.shutdown)
	}
	mp.shutdownLock.Unlock()
}

func (mp *Multiplex) IsClosed() bool {
	select {
	case <-mp.closed:
		return true
	default:
		return false
	}
}

func (mp *Multiplex) sendMsg(header uint64, data []byte, dl time.Time) error {
	mp.wrLock.Lock()
	defer mp.wrLock.Unlock()
	if !dl.IsZero() {
		if err := mp.con.SetWriteDeadline(dl); err != nil {
			return err
		}
	}
	n := binary.PutUvarint(mp.hdrBuf, header)
	n += binary.PutUvarint(mp.hdrBuf[n:], uint64(len(data)))
	_, err := mp.con.Write(mp.hdrBuf[:n])
	if err != nil {
		return err
	}

	if len(data) != 0 {
		_, err = mp.con.Write(data)
		if err != nil {
			return err
		}
	}
	if !dl.IsZero() {
		if err := mp.con.SetWriteDeadline(time.Time{}); err != nil {
			return err
		}
	}

	return nil
}

func (mp *Multiplex) nextChanID() (out uint64) {
	if mp.initiator {
		out = mp.nextID + 1
	} else {
		out = mp.nextID
	}
	mp.nextID += 2
	return
}

func (mp *Multiplex) NewStream() (*Stream, error) {
	return mp.NewNamedStream("")
}

func (mp *Multiplex) NewNamedStream(name string) (*Stream, error) {
	mp.chLock.Lock()

	// We could call IsClosed but this is faster (given that we already have
	// the lock).
	if mp.channels == nil {
		return nil, ErrShutdown
	}

	sid := mp.nextChanID()
	header := (sid << 3) | NewStream

	if name == "" {
		name = fmt.Sprint(sid)
	}
	s := mp.newStream(sid, name, true)
	mp.channels[sid] = s
	mp.chLock.Unlock()

	err := mp.sendMsg(header, []byte(name), time.Time{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (mp *Multiplex) cleanup() {
	mp.closeNoWait()
	mp.chLock.Lock()
	defer mp.chLock.Unlock()
	for _, msch := range mp.channels {
		msch.clLock.Lock()
		if !msch.closedRemote {
			msch.closedRemote = true
			// Cancel readers
			close(msch.dataIn)
		}
		msch.closedLocal = true
		msch.clLock.Unlock()
	}
	// Don't remove this nil assignment. We check if this is nil to check if
	// the connection is closed when we already have the lock (faster than
	// checking if the stream is closed).
	mp.channels = nil
	if mp.shutdownErr == nil {
		mp.shutdownErr = ErrShutdown
	}
	close(mp.closed)
}

func (mp *Multiplex) handleIncoming() {
	defer mp.cleanup()
	for {
		ch, tag, err := mp.readNextHeader()
		if err != nil {
			mp.shutdownErr = err
			return
		}

		b, err := mp.readNext()
		if err != nil {
			mp.shutdownErr = err
			return
		}

		mp.chLock.Lock()
		msch, ok := mp.channels[ch]
		mp.chLock.Unlock()
		switch tag {
		case NewStream:
			if ok {
				log.Debugf("received NewStream message for existing stream: %d", ch)
				continue
			}

			name := string(b)
			msch = mp.newStream(ch, name, false)
			mp.chLock.Lock()
			mp.channels[ch] = msch
			mp.chLock.Unlock()
			select {
			case mp.nstreams <- msch:
			case <-mp.shutdown:
				return
			}

		case Close:
			if !ok {
				continue
			}

			msch.clLock.Lock()

			if msch.closedRemote {
				msch.clLock.Unlock()
				continue
			}

			close(msch.dataIn)
			msch.closedRemote = true

			cleanup := msch.closedLocal

			msch.clLock.Unlock()

			if cleanup {
				mp.chLock.Lock()
				delete(mp.channels, ch)
				mp.chLock.Unlock()
			}
		default:
			if !ok {
				log.Debugf("message for non-existant stream, dropping data: %d", ch)
				continue
			}
			msch.clLock.Lock()
			remoteClosed := msch.closedRemote
			msch.clLock.Unlock()
			if remoteClosed {
				log.Errorf("Received data from remote after stream was closed by them. (len = %d)", len(b))
				continue
			}
			select {
			case msch.dataIn <- b:
			case <-mp.shutdown:
				return
			}
		}
	}
}

func (mp *Multiplex) readNextHeader() (uint64, uint64, error) {
	h, err := binary.ReadUvarint(mp.buf)
	if err != nil {
		return 0, 0, err
	}

	// get channel ID
	ch := h >> 3

	rem := h & 7

	return ch, rem, nil
}

func (mp *Multiplex) readNext() ([]byte, error) {
	// get length
	l, err := binary.ReadUvarint(mp.buf)
	if err != nil {
		return nil, err
	}

	if l > uint64(MaxMessageSize) {
		return nil, fmt.Errorf("message size too large!")
	}

	if l == 0 {
		return nil, nil
	}

	buf := mpool.ByteSlicePool.Get(uint32(l)).([]byte)[:l]
	n, err := io.ReadFull(mp.buf, buf)
	if err != nil {
		return nil, err
	}

	return buf[:n], nil
}
